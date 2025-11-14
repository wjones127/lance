# lance-memtest

Memory allocation testing utilities for Python test suites. This package provides tools to track memory allocations made by the Python interpreter and any Python libraries during test execution.

## Features

- **LD_PRELOAD-based interposition**: Intercepts all `malloc`, `free`, `calloc`, and `realloc` calls
- **Zero overhead when not tracking**: No performance impact unless explicitly enabled
- **Thread-safe statistics**: Uses atomic operations for accurate multi-threaded tracking
- **Python and CLI interfaces**: Use programmatically or from the command line
- **Comprehensive metrics**: Track allocations, deallocations, current usage, and peak memory

## Installation

### From source

```bash
cd memtest
maturin develop
```

### For development

```bash
cd memtest
make build
```

## Usage

### Python API

#### Basic tracking

```python
import memtest

# Reset statistics
memtest.reset_stats()

# Your code here
data = [0] * 1000000

# Get statistics
stats = memtest.get_stats()
print(f"Allocated: {stats['total_bytes_allocated']} bytes")
print(f"Peak usage: {stats['peak_bytes']} bytes")
```

#### Context manager

```python
import memtest

with memtest.track() as get_stats:
    # Allocate some memory
    data = [0] * 1000000

    # Get stats within the context
    stats = get_stats()
    print(f"Allocated: {stats['total_bytes_allocated']} bytes")
```

#### Pretty printing

```python
import memtest

# ... run some code ...

memtest.print_stats()
```

Output:
```
Memory Allocation Statistics:
  Total allocations:     1,234
  Total deallocations:   1,100
  Total bytes allocated: 128.5 KB
  Total bytes freed:     120.0 KB
  Current memory usage:  8.5 KB
  Peak memory usage:     15.2 KB
```

### Command Line Interface

#### Run a command with tracking

```bash
lance-memtest run python myscript.py
lance-memtest run pytest tests/
```

#### Get the library path

```bash
# Print path to the .so file
lance-memtest path

# Use with LD_PRELOAD manually
export LD_PRELOAD=$(lance-memtest path)
python myscript.py
```

#### View current statistics

```bash
lance-memtest stats
```

### Integration with pytest

```python
import pytest
import memtest

@pytest.fixture(autouse=True)
def track_memory():
    """Automatically track memory for all tests."""
    memtest.reset_stats()
    yield
    stats = memtest.get_stats()

    # Assert memory bounds
    assert stats['peak_bytes'] < 100 * 1024 * 1024, "Test used more than 100MB"

def test_my_function():
    result = my_function()

    # Check memory usage for this test
    stats = memtest.get_stats()
    print(f"Peak memory: {memtest.format_bytes(stats['peak_bytes'])}")
```

## Statistics

The following metrics are tracked:

- **`total_allocations`**: Total number of `malloc`/`calloc` calls
- **`total_deallocations`**: Total number of `free` calls
- **`total_bytes_allocated`**: Total bytes allocated across all calls
- **`total_bytes_deallocated`**: Total bytes freed across all calls
- **`current_bytes`**: Current memory usage (allocated - deallocated)
- **`peak_bytes`**: Peak memory usage observed

## How It Works

The package uses LD_PRELOAD to interpose the standard C library allocation functions (`malloc`, `free`, `calloc`, `realloc`). When these functions are called by Python or any C extension:

1. The interposed function records the allocation size
2. Statistics are updated using atomic operations (thread-safe)
3. The original libc function is called to perform the actual allocation

The Rust implementation ensures minimal overhead and uses a header-based approach to track allocation sizes.

## Limitations

- **Linux only**: LD_PRELOAD is a Linux-specific feature
- **Does not track Python object overhead**: Only tracks C-level allocations
- **Stack allocations not tracked**: Only heap allocations via malloc family
- **Reset affects all threads**: Statistics are global

## Development

### Build

```bash
make build
```

### Run tests

```bash
make test
```

### Format code

```bash
make format
```

### Lint

```bash
make lint
```

## Architecture

The package consists of:

1. **Rust interpose library** (`src/allocator.rs`): Interposes `malloc`/`free` family
2. **Statistics module** (`src/stats.rs`): Thread-safe atomic counters
3. **PyO3 bindings** (`src/lib.rs`): Exposes stats to Python
4. **Python wrapper** (`python/memtest/__init__.py`): High-level API
5. **CLI** (`python/memtest/__main__.py`): Command-line interface

## License

Apache-2.0
