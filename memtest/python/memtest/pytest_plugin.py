"""Pytest plugin for memory tracking during benchmarks.

This plugin provides a `memory_benchmark` fixture that wraps pytest-benchmark
to track memory allocations during the actual benchmark execution.

The plugin auto-detects if libmemtest.so is preloaded. If not, the
`memory_benchmark` fixture simply passes through to the regular `benchmark`
fixture.

Usage:
    def test_something(memory_benchmark):
        memory_benchmark(my_function, arg1, arg2)

Output:
    - Terminal summary with memory stats per benchmark
    - BMF JSON file for bencher.dev upload (--memory-json option)
"""

import json
from functools import wraps
from typing import Any, Callable, Dict, Optional

import pytest

from . import format_bytes, get_stats, is_preloaded, reset_stats

# Global storage for memory results across all tests
_memory_results: Dict[str, Dict[str, int]] = {}


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add command-line options for memory tracking."""
    group = parser.getgroup("memory", "memory tracking options")
    group.addoption(
        "--memory-json",
        action="store",
        default=None,
        metavar="PATH",
        help="Output path for memory stats JSON in Bencher Metric Format (BMF)",
    )


class MemoryTrackingBenchmark:
    """Wrapper around pytest-benchmark that tracks memory during execution."""

    def __init__(self, benchmark: Any, test_name: str):
        self._benchmark = benchmark
        self._test_name = test_name
        self._peak_memory = 0
        self._total_allocations = 0

    def _wrap_function(self, func: Callable) -> Callable:
        """Wrap a function to track memory around each invocation."""

        @wraps(func)
        def wrapper(*args, **kwargs):
            reset_stats()
            result = func(*args, **kwargs)
            stats = get_stats()
            # Track max peak across iterations
            self._peak_memory = max(self._peak_memory, stats["peak_bytes"])
            self._total_allocations += stats["total_allocations"]
            return result

        return wrapper

    def __call__(self, func: Callable, *args, **kwargs) -> Any:
        """Run benchmark with memory tracking."""
        wrapped = self._wrap_function(func)
        return self._benchmark(wrapped, *args, **kwargs)

    def pedantic(
        self,
        func: Callable,
        args: tuple = (),
        kwargs: Optional[Dict] = None,
        setup: Optional[Callable] = None,
        teardown: Optional[Callable] = None,
        rounds: int = 1,
        warmup_rounds: int = 0,
        iterations: int = 1,
    ) -> Any:
        """Run pedantic benchmark with memory tracking."""
        kwargs = kwargs or {}
        wrapped = self._wrap_function(func)
        return self._benchmark.pedantic(
            wrapped,
            args=args,
            kwargs=kwargs,
            setup=setup,
            teardown=teardown,
            rounds=rounds,
            warmup_rounds=warmup_rounds,
            iterations=iterations,
        )

    @property
    def group(self):
        return self._benchmark.group

    @group.setter
    def group(self, value):
        self._benchmark.group = value

    @property
    def name(self):
        return self._benchmark.name

    @property
    def extra_info(self):
        return self._benchmark.extra_info

    @extra_info.setter
    def extra_info(self, value):
        self._benchmark.extra_info = value

    def get_memory_stats(self) -> Dict[str, int]:
        """Get the collected memory statistics."""
        return {
            "peak_bytes": self._peak_memory,
            "total_allocations": self._total_allocations,
        }


@pytest.fixture
def memory_benchmark(benchmark, request):
    """Fixture that wraps benchmark to track memory during execution.

    If libmemtest.so is not preloaded, this fixture simply returns the
    regular benchmark fixture unchanged.

    Usage:
        def test_something(memory_benchmark):
            memory_benchmark(my_function, arg1, arg2)

        def test_pedantic(memory_benchmark):
            memory_benchmark.pedantic(my_function, rounds=5, iterations=10)
    """
    if not is_preloaded():
        # Not preloaded - just return regular benchmark
        yield benchmark
        return

    test_name = request.node.name
    tracker = MemoryTrackingBenchmark(benchmark, test_name)

    yield tracker

    # Store results after test completes
    stats = tracker.get_memory_stats()
    if stats["peak_bytes"] > 0 or stats["total_allocations"] > 0:
        _memory_results[test_name] = stats


def pytest_terminal_summary(terminalreporter, exitstatus: int, config) -> None:
    """Print memory statistics summary at the end of the test run."""
    if not _memory_results:
        return

    terminalreporter.write_sep("=", "Memory Statistics")

    # Calculate column widths
    name_width = max(len(name) for name in _memory_results.keys())
    name_width = max(name_width, len("Test"))

    # Header
    terminalreporter.write_line(
        f"{'Test':<{name_width}}  {'Peak Memory':>12}  {'Allocations':>12}"
    )
    terminalreporter.write_line("-" * (name_width + 28))

    # Results sorted by peak memory (descending)
    sorted_results = sorted(
        _memory_results.items(), key=lambda x: x[1]["peak_bytes"], reverse=True
    )

    for test_name, stats in sorted_results:
        peak = format_bytes(stats["peak_bytes"])
        allocs = f"{stats['total_allocations']:,}"
        terminalreporter.write_line(f"{test_name:<{name_width}}  {peak:>12}  {allocs:>12}")

    terminalreporter.write_line("")


def pytest_sessionfinish(session, exitstatus: int) -> None:
    """Write memory results to JSON file if --memory-json was specified."""
    if not _memory_results:
        return

    output_path = session.config.getoption("--memory-json")
    if not output_path:
        return

    # Convert to Bencher Metric Format (BMF)
    bmf_output = {}
    for test_name, stats in _memory_results.items():
        bmf_output[test_name] = {
            "peak_memory_bytes": {"value": stats["peak_bytes"]},
            "total_allocations": {"value": stats["total_allocations"]},
        }

    with open(output_path, "w") as f:
        json.dump(bmf_output, f, indent=2)
