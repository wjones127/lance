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
    #[link_name = "__libc_malloc"]
    fn libc_malloc( size: size_t ) -> *mut c_void;
    #[link_name = "__libc_calloc"]
    fn libc_calloc( count: size_t, element_size: size_t ) -> *mut c_void;
    #[link_name = "__libc_realloc"]
    fn libc_realloc( ptr: *mut c_void, size: size_t ) -> *mut c_void;
    #[link_name = "__libc_free"]
    fn libc_free( ptr: *mut c_void );
    #[link_name = "__libc_memalign"]
    fn libc_memalign( alignment: size_t, size: size_t ) -> *mut c_void;
}

// Implementations of standard allocation functions
// To track the size on free, we store the size at the start of the allocated block
// and return a pointer offset by the size of u64 (8 bytes).

fn extract(virtual_ptr: *mut c_void) -> (usize, *mut c_void) {
    let actual_ptr = (virtual_ptr as *mut u8).sub(8) as *mut u8;
    let size_ptr = actual_ptr as *mut u64;
    let size = unsafe { *size_ptr } as usize;
    (size, actual_ptr as *mut c_void)
}

/// Take a allocated pointer and size, store the size, and return the adjusted pointer
fn to_virtual(actual_ptr: *mut c_void, size: usize) -> *mut c_void {
    if actual_ptr.is_null() {
        return std::ptr::null_mut();
    }
    let ptr = actual_ptr as *mut u8;
    unsafe {
        *(ptr as *mut u64) = size as u64;
    }
    ptr.add(8) as *mut c_void
}

#[no_mangle]
pub unsafe extern "C" fn malloc(size: size_t) -> *mut c_void {
    STATS.record_allocation(size);
    to_virtual(libc_malloc(size + 8), size)
}

#[no_mangle]
pub unsafe extern "C" fn calloc(size: size_t, element_size: size_t) -> *mut c_void {
    let Some(total_size) = size.checked_mul(element_size) else {
        return std::ptr::null_mut();
    };
    STATS.record_allocation(total_size);
    to_virtual(libc_calloc(total_size + 8, 1), total_size)
}

#[no_mangle]
pub unsafe extern "C" fn free(ptr: *mut c_void) {
    let actual_ptr = if ptr.is_null() {
        return;
    } else {
        (ptr as *mut u8).sub(8) as *mut c_void
    };
    let (size, )
    let size_ptr = (actual_ptr as *mut u8) as *mut u64;
    let size = *size_ptr as size_t;
    STATS.record_deallocation(size);
    libc_free(actual_ptr as *mut c_void);
}

#[no_mangle]
pub unsafe extern "C" fn realloc(ptr: *mut c_void, size: size_t) -> *mut c_void {
    let actual_ptr = if ptr.is_null() {
        ptr
    } else {
        (ptr as *mut u8).sub(8) as *mut c_void
    };
    let old_size = if !ptr.is_null() {
        let size_ptr = (actual_ptr as *mut u8) as *mut u64;
        *size_ptr as size_t
    } else {
        0
    };
    STATS.record_deallocation(old_size);
    STATS.record_allocation(size);
    let new_ptr = libc_realloc(actual_ptr, size + 8);
    if new_ptr.is_null() {
        return std::ptr::null_mut();
    }
    new_ptr.add(8) as *mut c_void
}

#[no_mangle]
pub unsafe extern "C" fn memalign(alignment: size_t, size: size_t) -> *mut c_void {
    STATS.record_allocation(size);
    libc_memalign(alignment, size)
}

#[no_mangle]
pub unsafe extern "C" fn posix_memalign(
    memptr: *mut *mut c_void,
    alignment: size_t,
    size: size_t,
) -> i32 {
    let ptr = libc_memalign(alignment, size);
    if ptr.is_null() {
        return libc::ENOMEM;
    }
    STATS.record_allocation(size);
    *memptr = ptr;
    0
}

#[no_mangle]
pub unsafe extern "C" fn aligned_alloc(alignment: size_t, size: size_t) -> *mut c_void {
    // Do we need to adjust this for alignment?
    let effective_size = size + 8;
    STATS.record_allocation(size);
    let size = libc_memalign(alignment, size);
}

#[no_mangle]
pub unsafe extern "C" fn valloc(size: size_t) -> *mut c_void {
    STATS.record_allocation(size);
    libc_memalign(libc::sysconf(libc::_SC_PAGESIZE) as size_t, size)
}

#[no_mangle]
pub unsafe extern "C" fn reallocarray( old_ptr: *mut c_void, count: size_t, element_size: size_t ) -> *mut c_void {
    let size = count.checked_mul(element_size);
    if size.is_none() {
        return std::ptr::null_mut();
    }
    let size = size.unwrap();
    STATS.record_allocation(size);
    libc_realloc( old_ptr, size )
}
