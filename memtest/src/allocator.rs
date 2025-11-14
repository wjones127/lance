use crate::stats::STATS;
use libc::{c_void, size_t};
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};

type MallocFn = unsafe extern "C" fn(size_t) -> *mut c_void;
type FreeFn = unsafe extern "C" fn(*mut c_void);
type CallocFn = unsafe extern "C" fn(size_t, size_t) -> *mut c_void;
type ReallocFn = unsafe extern "C" fn(*mut c_void, size_t) -> *mut c_void;

static REAL_MALLOC: AtomicPtr<c_void> = AtomicPtr::new(std::ptr::null_mut());
static REAL_FREE: AtomicPtr<c_void> = AtomicPtr::new(std::ptr::null_mut());
static REAL_CALLOC: AtomicPtr<c_void> = AtomicPtr::new(std::ptr::null_mut());
static REAL_REALLOC: AtomicPtr<c_void> = AtomicPtr::new(std::ptr::null_mut());
static INITIALIZING: AtomicBool = AtomicBool::new(false);
static INITIALIZED: AtomicBool = AtomicBool::new(false);

const RTLD_NEXT: *mut c_void = -1isize as *mut c_void;

extern "C" {
    fn dlsym(handle: *mut c_void, symbol: *const libc::c_char) -> *mut c_void;
}

/// Initialize the function pointers to the real allocation functions
unsafe fn init_real_functions() {
    // If already initialized, return
    if INITIALIZED.load(Ordering::Acquire) {
        return;
    }

    // Try to set initializing flag
    if INITIALIZING
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        // Someone else is initializing, spin wait
        while !INITIALIZED.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        return;
    }

    // We're the one initializing
    let malloc_ptr = dlsym(RTLD_NEXT, c"malloc".as_ptr() as *const libc::c_char);
    let free_ptr = dlsym(RTLD_NEXT, c"free".as_ptr() as *const libc::c_char);
    let calloc_ptr = dlsym(RTLD_NEXT, c"calloc".as_ptr() as *const libc::c_char);
    let realloc_ptr = dlsym(RTLD_NEXT, c"realloc".as_ptr() as *const libc::c_char);

    REAL_MALLOC.store(malloc_ptr, Ordering::Release);
    REAL_FREE.store(free_ptr, Ordering::Release);
    REAL_CALLOC.store(calloc_ptr, Ordering::Release);
    REAL_REALLOC.store(realloc_ptr, Ordering::Release);

    INITIALIZED.store(true, Ordering::Release);
}

/// Store allocation size in a header before the returned pointer
#[repr(C)]
struct AllocationHeader {
    size: usize,
}

const HEADER_SIZE: usize = std::mem::size_of::<AllocationHeader>();

#[no_mangle]
pub unsafe extern "C" fn malloc(size: size_t) -> *mut c_void {
    // If we're currently initializing, forward directly to avoid recursion
    if INITIALIZING.load(Ordering::Acquire) && !INITIALIZED.load(Ordering::Acquire) {
        // During initialization, dlsym might call malloc
        // We can't use RTLD_NEXT here, so we'll just use a simple bump allocator
        // or return null and hope dlsym handles it
        return std::ptr::null_mut();
    }

    init_real_functions();

    let malloc_ptr = REAL_MALLOC.load(Ordering::Acquire);
    if malloc_ptr.is_null() {
        return std::ptr::null_mut();
    }

    let real_malloc: MallocFn = std::mem::transmute(malloc_ptr);
    let total_size = size.saturating_add(HEADER_SIZE);
    let ptr = real_malloc(total_size);

    if !ptr.is_null() {
        // Store size in header
        let header = ptr as *mut AllocationHeader;
        (*header).size = size;

        STATS.record_allocation(size);

        // Return pointer after header
        ptr.add(HEADER_SIZE)
    } else {
        ptr
    }
}

#[no_mangle]
pub unsafe extern "C" fn free(ptr: *mut c_void) {
    if ptr.is_null() {
        return;
    }

    // If called during initialization, do nothing (malloc returned null anyway)
    if INITIALIZING.load(Ordering::Acquire) && !INITIALIZED.load(Ordering::Acquire) {
        return;
    }

    init_real_functions();

    let free_ptr = REAL_FREE.load(Ordering::Acquire);
    if free_ptr.is_null() {
        return;
    }

    let real_free: FreeFn = std::mem::transmute(free_ptr);

    // Get the actual allocation pointer (before header)
    let actual_ptr = (ptr as *mut u8).sub(HEADER_SIZE);
    let header = actual_ptr as *mut AllocationHeader;
    let size = (*header).size;

    STATS.record_deallocation(size);

    real_free(actual_ptr as *mut c_void);
}

#[no_mangle]
pub unsafe extern "C" fn calloc(nmemb: size_t, size: size_t) -> *mut c_void {
    if INITIALIZING.load(Ordering::Acquire) && !INITIALIZED.load(Ordering::Acquire) {
        return std::ptr::null_mut();
    }

    init_real_functions();

    let calloc_ptr = REAL_CALLOC.load(Ordering::Acquire);
    if calloc_ptr.is_null() {
        return std::ptr::null_mut();
    }

    let real_calloc: CallocFn = std::mem::transmute(calloc_ptr);
    let total_size = nmemb.saturating_mul(size);
    let allocation_size = total_size.saturating_add(HEADER_SIZE);

    let ptr = real_calloc(allocation_size, 1);

    if !ptr.is_null() {
        let header = ptr as *mut AllocationHeader;
        (*header).size = total_size;

        STATS.record_allocation(total_size);

        ptr.add(HEADER_SIZE)
    } else {
        ptr
    }
}

#[no_mangle]
pub unsafe extern "C" fn realloc(ptr: *mut c_void, size: size_t) -> *mut c_void {
    if INITIALIZING.load(Ordering::Acquire) && !INITIALIZED.load(Ordering::Acquire) {
        return std::ptr::null_mut();
    }

    init_real_functions();

    let realloc_ptr = REAL_REALLOC.load(Ordering::Acquire);
    if realloc_ptr.is_null() {
        return std::ptr::null_mut();
    }

    let real_realloc: ReallocFn = std::mem::transmute(realloc_ptr);

    if ptr.is_null() {
        // realloc(NULL, size) is equivalent to malloc(size)
        return malloc(size);
    }

    if size == 0 {
        // realloc(ptr, 0) is equivalent to free(ptr)
        free(ptr);
        return std::ptr::null_mut();
    }

    // Get old size from header
    let actual_ptr = (ptr as *mut u8).sub(HEADER_SIZE);
    let old_header = actual_ptr as *mut AllocationHeader;
    let old_size = (*old_header).size;

    // Reallocate with new size
    let total_size = size.saturating_add(HEADER_SIZE);
    let new_ptr = real_realloc(actual_ptr as *mut c_void, total_size);

    if !new_ptr.is_null() {
        // Update header with new size
        let new_header = new_ptr as *mut AllocationHeader;
        (*new_header).size = size;

        // Record the change in allocation
        STATS.record_deallocation(old_size);
        STATS.record_allocation(size);

        new_ptr.add(HEADER_SIZE)
    } else {
        new_ptr
    }
}
