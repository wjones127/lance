use crate::stats::STATS;
use libc::{c_void, size_t};
use std::cell::Cell;
use std::sync::Once;

type MallocFn = unsafe extern "C" fn(size_t) -> *mut c_void;
type FreeFn = unsafe extern "C" fn(*mut c_void);
type CallocFn = unsafe extern "C" fn(size_t, size_t) -> *mut c_void;
type ReallocFn = unsafe extern "C" fn(*mut c_void, size_t) -> *mut c_void;

static INIT: Once = Once::new();
static mut REAL_MALLOC: Option<MallocFn> = None;
static mut REAL_FREE: Option<FreeFn> = None;
static mut REAL_CALLOC: Option<CallocFn> = None;
static mut REAL_REALLOC: Option<ReallocFn> = None;

const RTLD_NEXT: *mut c_void = -1isize as *mut c_void;

thread_local! {
    static IN_HOOK: Cell<bool> = const { Cell::new(false) };
}

extern "C" {
    fn dlsym(handle: *mut c_void, symbol: *const libc::c_char) -> *mut c_void;
}

/// Initialize the function pointers to the real allocation functions
unsafe fn init_real_functions() {
    INIT.call_once(|| {
        // Prevent recursion during initialization
        IN_HOOK.with(|flag| flag.set(true));

        REAL_MALLOC = Some(std::mem::transmute(dlsym(
            RTLD_NEXT,
            b"malloc\0".as_ptr() as *const libc::c_char,
        )));
        REAL_FREE = Some(std::mem::transmute(dlsym(
            RTLD_NEXT,
            b"free\0".as_ptr() as *const libc::c_char,
        )));
        REAL_CALLOC = Some(std::mem::transmute(dlsym(
            RTLD_NEXT,
            b"calloc\0".as_ptr() as *const libc::c_char,
        )));
        REAL_REALLOC = Some(std::mem::transmute(dlsym(
            RTLD_NEXT,
            b"realloc\0".as_ptr() as *const libc::c_char,
        )));

        IN_HOOK.with(|flag| flag.set(false));
    });
}

/// Store allocation size in a header before the returned pointer
#[repr(C)]
struct AllocationHeader {
    size: usize,
}

const HEADER_SIZE: usize = std::mem::size_of::<AllocationHeader>();

#[no_mangle]
pub unsafe extern "C" fn malloc(size: size_t) -> *mut c_void {
    // Check if we're already in a hook to prevent recursion
    let in_hook = IN_HOOK.with(|flag| {
        if flag.get() {
            true
        } else {
            flag.set(true);
            false
        }
    });

    if in_hook {
        // We're in recursion, just call the real malloc
        init_real_functions();
        if let Some(real_malloc) = REAL_MALLOC {
            return real_malloc(size);
        }
        return std::ptr::null_mut();
    }

    init_real_functions();

    let result = if let Some(real_malloc) = REAL_MALLOC {
        let total_size = size + HEADER_SIZE;
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
    } else {
        std::ptr::null_mut()
    };

    IN_HOOK.with(|flag| flag.set(false));
    result
}

#[no_mangle]
pub unsafe extern "C" fn free(ptr: *mut c_void) {
    if ptr.is_null() {
        return;
    }

    // Check if we're already in a hook to prevent recursion
    let in_hook = IN_HOOK.with(|flag| {
        if flag.get() {
            true
        } else {
            flag.set(true);
            false
        }
    });

    init_real_functions();

    if let Some(real_free) = REAL_FREE {
        if in_hook {
            // We're in recursion, just call the real free
            real_free(ptr);
            return;
        }

        // Get the actual allocation pointer (before header)
        let actual_ptr = (ptr as *mut u8).sub(HEADER_SIZE);
        let header = actual_ptr as *mut AllocationHeader;
        let size = (*header).size;

        STATS.record_deallocation(size);

        real_free(actual_ptr as *mut c_void);

        IN_HOOK.with(|flag| flag.set(false));
    }
}

#[no_mangle]
pub unsafe extern "C" fn calloc(nmemb: size_t, size: size_t) -> *mut c_void {
    let in_hook = IN_HOOK.with(|flag| {
        if flag.get() {
            true
        } else {
            flag.set(true);
            false
        }
    });

    if in_hook {
        init_real_functions();
        if let Some(real_calloc) = REAL_CALLOC {
            return real_calloc(nmemb, size);
        }
        return std::ptr::null_mut();
    }

    init_real_functions();

    let result = if let Some(real_calloc) = REAL_CALLOC {
        let total_size = nmemb * size;
        let allocation_size = total_size + HEADER_SIZE;

        let ptr = real_calloc(allocation_size, 1);

        if !ptr.is_null() {
            let header = ptr as *mut AllocationHeader;
            (*header).size = total_size;

            STATS.record_allocation(total_size);

            ptr.add(HEADER_SIZE)
        } else {
            ptr
        }
    } else {
        std::ptr::null_mut()
    };

    IN_HOOK.with(|flag| flag.set(false));
    result
}

#[no_mangle]
pub unsafe extern "C" fn realloc(ptr: *mut c_void, size: size_t) -> *mut c_void {
    let in_hook = IN_HOOK.with(|flag| {
        if flag.get() {
            true
        } else {
            flag.set(true);
            false
        }
    });

    if in_hook {
        init_real_functions();
        if let Some(real_realloc) = REAL_REALLOC {
            return real_realloc(ptr, size);
        }
        return std::ptr::null_mut();
    }

    init_real_functions();

    let result = if let Some(real_realloc) = REAL_REALLOC {
        if ptr.is_null() {
            // realloc(NULL, size) is equivalent to malloc(size)
            // Note: This will set the flag again, but that's handled
            IN_HOOK.with(|flag| flag.set(false));
            return malloc(size);
        }

        if size == 0 {
            // realloc(ptr, 0) is equivalent to free(ptr)
            IN_HOOK.with(|flag| flag.set(false));
            free(ptr);
            return std::ptr::null_mut();
        }

        // Get old size from header
        let actual_ptr = (ptr as *mut u8).sub(HEADER_SIZE);
        let old_header = actual_ptr as *mut AllocationHeader;
        let old_size = (*old_header).size;

        // Reallocate with new size
        let total_size = size + HEADER_SIZE;
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
    } else {
        std::ptr::null_mut()
    };

    IN_HOOK.with(|flag| flag.set(false));
    result
}
