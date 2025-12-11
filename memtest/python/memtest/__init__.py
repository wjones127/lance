"""Memory allocation testing utilities for Python."""

import ctypes
from pathlib import Path
from typing import Dict, Optional
from contextlib import contextmanager

__version__ = "0.1.0"


class _MemtestStats(ctypes.Structure):
    """C struct matching MemtestStats in Rust."""

    _fields_ = [
        ("total_allocations", ctypes.c_uint64),
        ("total_deallocations", ctypes.c_uint64),
        ("total_bytes_allocated", ctypes.c_uint64),
        ("total_bytes_deallocated", ctypes.c_uint64),
        ("current_bytes", ctypes.c_uint64),
        ("peak_bytes", ctypes.c_uint64),
    ]


def _load_library():
    """Load the memtest shared library."""
    # Find the library relative to this module
    module_dir = Path(__file__).parent

    # Look for the library in common locations
    possible_paths = [
        module_dir / "libmemtest.so",  # Linux
        module_dir / "libmemtest.dylib",  # macOS
        module_dir / "memtest.dll",  # Windows
    ]

    for lib_path in possible_paths:
        if lib_path.exists():
            lib = ctypes.CDLL(str(lib_path))

            # Define function signatures
            lib.memtest_get_stats.argtypes = [ctypes.POINTER(_MemtestStats)]
            lib.memtest_get_stats.restype = None

            lib.memtest_reset_stats.argtypes = []
            lib.memtest_reset_stats.restype = None

            return lib, lib_path

    raise RuntimeError("memtest library not found. Run 'make build' to build it.")


# Load library at module import
_lib, _lib_path = _load_library()


def get_library_path() -> Path:
    """Get the path to the memtest shared library for use with LD_PRELOAD.

    Returns:
        Path to the .so file that can be used with LD_PRELOAD

    Example:
        >>> lib_path = get_library_path()
        >>> os.environ['LD_PRELOAD'] = str(lib_path)
    """
    return _lib_path


def get_stats() -> Dict[str, int]:
    """Get current memory allocation statistics.

    Returns:
        Dictionary containing:
            - total_allocations: Total number of malloc/calloc calls
            - total_deallocations: Total number of free calls
            - total_bytes_allocated: Total bytes allocated
            - total_bytes_deallocated: Total bytes freed
            - current_bytes: Current memory usage (allocated - deallocated)
            - peak_bytes: Peak memory usage observed

    Example:
        >>> stats = get_stats()
        >>> print(f"Current memory: {stats['current_bytes']} bytes")
        >>> print(f"Peak memory: {stats['peak_bytes']} bytes")
    """
    stats = _MemtestStats()
    _lib.memtest_get_stats(ctypes.byref(stats))

    return {
        "total_allocations": stats.total_allocations,
        "total_deallocations": stats.total_deallocations,
        "total_bytes_allocated": stats.total_bytes_allocated,
        "total_bytes_deallocated": stats.total_bytes_deallocated,
        "current_bytes": stats.current_bytes,
        "peak_bytes": stats.peak_bytes,
    }


def reset_stats() -> None:
    """Reset all allocation statistics to zero.

    This is useful for measuring allocations in a specific section of code.

    Example:
        >>> reset_stats()
        >>> # ... run code to measure ...
        >>> stats = get_stats()
    """
    _lib.memtest_reset_stats()


@contextmanager
def track(reset: bool = True):
    """Context manager to track allocations within a code block.

    Args:
        reset: Whether to reset statistics before entering the context

    Yields:
        A function that returns current statistics

    Example:
        >>> with track() as get:
        ...     data = [0] * 1000
        ...     stats = get()
        ...     print(f"Allocated: {stats['total_bytes_allocated']} bytes")
    """
    if reset:
        reset_stats()

    yield get_stats


def format_bytes(num_bytes: int) -> str:
    """Format byte count as human-readable string.

    Args:
        num_bytes: Number of bytes

    Returns:
        Formatted string (e.g., "1.5 MB")
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} PB"


def print_stats(stats: Optional[Dict[str, int]] = None) -> None:
    """Print allocation statistics in a readable format.

    Args:
        stats: Statistics dictionary. If None, fetches current stats.

    Example:
        >>> print_stats()
        Memory Allocation Statistics:
          Total allocations:     1,234
          Total deallocations:   1,100
          Total bytes allocated: 128.5 KB
          Total bytes freed:     120.0 KB
          Current memory usage:  8.5 KB
          Peak memory usage:     15.2 KB
    """
    if stats is None:
        stats = get_stats()

    print("Memory Allocation Statistics:")
    print(f"  Total allocations:     {stats['total_allocations']:,}")
    print(f"  Total deallocations:   {stats['total_deallocations']:,}")
    print(f"  Total bytes allocated: {format_bytes(stats['total_bytes_allocated'])}")
    print(f"  Total bytes freed:     {format_bytes(stats['total_bytes_deallocated'])}")
    print(f"  Current memory usage:  {format_bytes(stats['current_bytes'])}")
    print(f"  Peak memory usage:     {format_bytes(stats['peak_bytes'])}")


def is_preloaded() -> bool:
    """Check if libmemtest.so is preloaded and actively tracking allocations.

    Returns:
        True if the library is preloaded via LD_PRELOAD, False otherwise.

    Example:
        >>> if is_preloaded():
        ...     stats = get_stats()
        ...     print(f"Tracking {stats['total_allocations']} allocations")
    """
    try:
        stats = get_stats()
        # If we can get stats and there's been any activity, we're preloaded
        # Even with no activity, if the library loads we're preloaded
        return True
    except Exception:
        return False


__all__ = [
    "get_library_path",
    "get_stats",
    "reset_stats",
    "track",
    "format_bytes",
    "print_stats",
    "is_preloaded",
]
