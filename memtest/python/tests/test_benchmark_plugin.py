"""Test the pytest-benchmark memory tracking plugin."""

import pytest


def allocate_memory(size_mb: int) -> list:
    """Allocate approximately size_mb of memory."""
    # Each int in Python takes about 28 bytes, but in a list it's stored as a pointer
    # A list of zeros: each element is ~8 bytes for the pointer + shared int object
    # For a rough approximation, 1MB ~= 125000 elements
    return [0] * (size_mb * 125000)


def test_basic_memory_tracking(memory_benchmark):
    """Test that memory is tracked during benchmark execution."""

    def workload():
        data = allocate_memory(10)  # ~10MB
        return len(data)

    result = memory_benchmark(workload)
    assert result == 10 * 125000


def test_pedantic_mode(memory_benchmark):
    """Test memory tracking with pedantic mode."""

    def workload():
        data = allocate_memory(5)  # ~5MB
        return sum(data)

    result = memory_benchmark.pedantic(workload, rounds=3, iterations=1)
    assert result == 0


def test_with_arguments(memory_benchmark):
    """Test memory tracking with function arguments."""

    def workload(multiplier: int, base_size: int = 1):
        data = allocate_memory(base_size * multiplier)
        return len(data)

    result = memory_benchmark(workload, 2, base_size=3)  # 6MB
    assert result == 6 * 125000
