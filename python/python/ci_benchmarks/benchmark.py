# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""
Custom benchmark infrastructure for tracking IO and memory stats.

This module provides an `io_memory_benchmark` marker and fixture that tracks:
- Peak memory usage
- Total allocations
- Read IOPS and bytes
- Write IOPS and bytes

Usage:
    @pytest.mark.io_memory_benchmark()
    def test_something(benchmark):
        def workload(dataset):
            dataset.to_table()
        benchmark(workload, dataset)
"""

import json
import subprocess
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pytest

# Try to import memtest, but don't fail if not available
try:
    import memtest

    MEMTEST_AVAILABLE = memtest.is_preloaded()
except ImportError:
    MEMTEST_AVAILABLE = False


@dataclass
class BenchmarkStats:
    """Statistics collected during a benchmark run."""

    # Memory stats (only populated if memtest is preloaded)
    peak_bytes: int = 0
    total_allocations: int = 0

    # IO stats
    read_iops: int = 0
    read_bytes: int = 0
    write_iops: int = 0
    write_bytes: int = 0


@dataclass
class BenchmarkResult:
    """Result of a single benchmark test."""

    name: str
    stats: BenchmarkStats


# Global storage for benchmark results
_benchmark_results: List[BenchmarkResult] = []


def _format_bytes(num_bytes: int) -> str:
    """Format byte count as human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} PB"


def _format_count(count: int) -> str:
    """Format a large count with commas."""
    for unit in ["", "K"]:
        if abs(count) < 1000.0:
            return f"{count:.1f} {unit}"
        count /= 1000.0
    return f"{count:.1f} M"


# --- Baseline save/compare infrastructure ---

BENCHMARKS_DIR = Path(".benchmarks")


@dataclass
class BaselineMetadata:
    """Metadata about a saved baseline."""

    timestamp: str
    git_commit: str
    git_branch: str
    name: str


@dataclass
class Baseline:
    """A saved baseline with metadata and benchmark results."""

    metadata: BaselineMetadata
    benchmarks: Dict[str, BenchmarkStats]


def _get_git_info() -> tuple:
    """Get current git commit and branch."""
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        commit = "unknown"

    try:
        branch = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        branch = "unknown"

    return commit, branch


def _get_baseline_path(name: str) -> Path:
    """Get the path for a baseline file."""
    return BENCHMARKS_DIR / f"{name}.json"


def _save_baseline(name: str, results: List[BenchmarkResult]) -> Path:
    """Save benchmark results as a baseline."""
    BENCHMARKS_DIR.mkdir(exist_ok=True)

    commit, branch = _get_git_info()
    metadata = BaselineMetadata(
        timestamp=datetime.now(timezone.utc).isoformat(),
        git_commit=commit,
        git_branch=branch,
        name=name,
    )

    benchmarks = {}
    for result in results:
        benchmarks[result.name] = asdict(result.stats)

    baseline_data = {
        "metadata": asdict(metadata),
        "benchmarks": benchmarks,
    }

    path = _get_baseline_path(name)
    with open(path, "w") as f:
        json.dump(baseline_data, f, indent=2)

    return path


def _load_baseline(name: str) -> Optional[Baseline]:
    """Load a saved baseline. Returns None if not found."""
    path = _get_baseline_path(name)
    if not path.exists():
        return None

    with open(path) as f:
        data = json.load(f)

    metadata = BaselineMetadata(**data["metadata"])
    benchmarks = {}
    for test_name, stats_dict in data["benchmarks"].items():
        benchmarks[test_name] = BenchmarkStats(**stats_dict)

    return Baseline(metadata=metadata, benchmarks=benchmarks)


def _format_delta(current: int, baseline: int) -> str:
    """Format a delta as a percentage string."""
    if baseline == 0:
        if current == 0:
            return "+0%"
        return "+inf%"
    pct = ((current - baseline) / baseline) * 100
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.0f}%"


def _format_iops(iops: int) -> str:
    """Format IOPS count."""
    return f"{iops:,}"


class IOMemoryBenchmark:
    """Benchmark fixture that tracks IO and memory during execution."""

    def __init__(self, test_name: str):
        self._test_name = test_name
        self._stats = BenchmarkStats()

    def __call__(
        self,
        func: Callable,
        dataset: Any,
        warmup: bool = True,
    ) -> Any:
        """
        Run a benchmark function with IO and memory tracking.

        Parameters
        ----------
        func : Callable
            The function to benchmark. Should accept a dataset as first argument.
        dataset : lance.LanceDataset
            The dataset to pass to the function.
        warmup : bool, default True
            Whether to run a warmup iteration before measuring.

        Returns
        -------
        Any
            The return value of the benchmark function.
        """
        # Warmup run (not measured)
        if warmup:
            func(dataset)

        # Reset IO stats before the measured run
        dataset.io_stats_incremental()

        # Run with memory tracking if available
        if MEMTEST_AVAILABLE:
            memtest.reset_stats()
            result = func(dataset)
            mem_stats = memtest.get_stats()
            self._stats.peak_bytes = mem_stats["peak_bytes"]
            self._stats.total_allocations = mem_stats["total_allocations"]
        else:
            result = func(dataset)

        # Capture IO stats
        io_stats = dataset.io_stats_incremental()
        self._stats.read_iops = io_stats.read_iops
        self._stats.read_bytes = io_stats.read_bytes
        self._stats.write_iops = io_stats.write_iops
        self._stats.write_bytes = io_stats.written_bytes

        return result

    def get_stats(self) -> BenchmarkStats:
        """Get the collected statistics."""
        return self._stats


@pytest.fixture
def io_mem_benchmark(request):
    """
    Fixture that provides IO and memory benchmarking.

    Only active for tests marked with @pytest.mark.io_memory_benchmark().
    For other tests, returns a no-op benchmark that just calls the function.

    Usage:
        @pytest.mark.io_memory_benchmark()
        def test_something(io_mem_benchmark):
            def workload(dataset):
                dataset.to_table()
            io_mem_benchmark(workload, dataset)
    """
    marker = request.node.get_closest_marker("io_memory_benchmark")

    if marker is None:
        # Not an io_memory_benchmark test, return a simple passthrough
        class PassthroughBenchmark:
            def __call__(self, func, dataset, warmup=True):
                return func(dataset)

        yield PassthroughBenchmark()
        return

    test_name = request.node.name
    tracker = IOMemoryBenchmark(test_name)

    yield tracker

    # Store results after test completes
    stats = tracker.get_stats()
    _benchmark_results.append(BenchmarkResult(name=test_name, stats=stats))


def pytest_configure(config):
    """Register the io_memory_benchmark marker."""
    config.addinivalue_line(
        "markers",
        "io_memory_benchmark(): Mark test as an IO/memory benchmark",
    )


def pytest_addoption(parser):
    """Add command-line options for benchmark output."""
    group = parser.getgroup("io_memory_benchmark", "IO/memory benchmark options")
    group.addoption(
        "--benchmark-stats-json",
        action="store",
        default=None,
        metavar="PATH",
        help="Output path for benchmark stats JSON in Bencher Metric Format (BMF)",
    )
    group.addoption(
        "--iom-save",
        action="store",
        default=None,
        metavar="NAME",
        help="Save IO/memory benchmark results to .benchmarks/{NAME}.json",
    )
    group.addoption(
        "--iom-compare",
        action="store",
        default=None,
        metavar="NAME",
        help="Compare IO/memory results against saved baseline .benchmarks/{NAME}.json",
    )


def _print_summary_no_compare(terminalreporter, results: List[BenchmarkResult]):
    """Print benchmark summary without comparison."""
    name_width = max(len(r.name) for r in results)
    name_width = max(name_width, len("Test"))

    if MEMTEST_AVAILABLE:
        terminalreporter.write_line(
            f"{'Test':<{name_width}}  {'Peak Mem':>10}  {'Allocs':>10}  "
            f"{'Read IOPS':>10}  {'Read Bytes':>12}  "
            f"{'Write IOPS':>10}  {'Write Bytes':>12}"
        )
        terminalreporter.write_line("-" * (name_width + 76))
    else:
        terminalreporter.write_line(
            f"{'Test':<{name_width}}  "
            f"{'Read IOPS':>10}  {'Read Bytes':>12}  "
            f"{'Write IOPS':>10}  {'Write Bytes':>12}"
        )
        terminalreporter.write_line("-" * (name_width + 52))

    sorted_results = sorted(results, key=lambda r: r.stats.read_bytes, reverse=True)

    for result in sorted_results:
        s = result.stats
        if MEMTEST_AVAILABLE:
            terminalreporter.write_line(
                f"{result.name:<{name_width}}  "
                f"{_format_bytes(s.peak_bytes):>10}  "
                f"{_format_count(s.total_allocations):>10}  "
                f"{s.read_iops:>10,}  "
                f"{_format_bytes(s.read_bytes):>12}  "
                f"{s.write_iops:>10,}  "
                f"{_format_bytes(s.write_bytes):>12}"
            )
        else:
            terminalreporter.write_line(
                f"{result.name:<{name_width}}  "
                f"{s.read_iops:>10,}  "
                f"{_format_bytes(s.read_bytes):>12}  "
                f"{s.write_iops:>10,}  "
                f"{_format_bytes(s.write_bytes):>12}"
            )


# ANSI color codes
_GREEN = "\033[32m"
_RED = "\033[31m"
_RESET = "\033[0m"


def _col(value: str, delta: str, vw: int, dw: int, color: str = "") -> str:
    """Format a column with right-aligned value and optionally colored delta."""
    if color:
        return f"{value:>{vw}} {color}{delta:<{dw}}{_RESET}"
    return f"{value:>{vw}} {delta:<{dw}}"


def _delta_color(current: int, baseline: int, use_color: bool) -> str:
    """Return ANSI color code for delta (green=improvement, red=regression)."""
    if not use_color or current == baseline:
        return ""
    # For memory/IO metrics, lower is better
    return _GREEN if current < baseline else _RED


def _print_summary_with_compare(
    terminalreporter, results: List[BenchmarkResult], baseline: Baseline
):
    """Print benchmark summary with comparison against baseline."""
    meta = baseline.metadata
    ts = datetime.fromisoformat(meta.timestamp).strftime("%Y-%m-%d %H:%M")
    terminalreporter.write_line(
        f"Comparing against: {meta.name} ({ts}, commit {meta.git_commit})"
    )
    terminalreporter.write_line("")

    name_width = max(len(r.name) for r in results)
    name_width = max(name_width, len("Test"))

    # Column widths: value portion and delta portion separately for alignment
    vw = 10  # Width for the value (e.g., "13.8 MB")
    dw = 7  # Width for the delta (e.g., "(+2%)")
    cw = vw + 1 + dw  # Total column width

    if MEMTEST_AVAILABLE:
        terminalreporter.write_line(
            f"{'Test':<{name_width}}  {'Peak Mem':>{cw}}  "
            f"{'Allocs':>{cw}}  {'Read IOPS':>{cw}}  {'Read Bytes':>{cw}}"
        )
        terminalreporter.write_line("-" * (name_width + 4 * cw + 8))
    else:
        terminalreporter.write_line(
            f"{'Test':<{name_width}}  {'Read IOPS':>{cw}}  {'Read Bytes':>{cw}}"
        )
        terminalreporter.write_line("-" * (name_width + 2 * cw + 4))

    sorted_results = sorted(results, key=lambda r: r.stats.read_bytes, reverse=True)
    use_color = terminalreporter.hasmarkup

    for result in sorted_results:
        s = result.stats
        b = baseline.benchmarks.get(result.name)

        if b is None:
            # New test, not in baseline
            cols = []
            if MEMTEST_AVAILABLE:
                cols.append(_col(_format_bytes(s.peak_bytes), "(new)", vw, dw))
                cols.append(_col(_format_count(s.total_allocations), "(new)", vw, dw))
            cols.append(_col(_format_iops(s.read_iops), "(new)", vw, dw))
            cols.append(_col(_format_bytes(s.read_bytes), "(new)", vw, dw))
            terminalreporter.write_line(
                f"{result.name:<{name_width}}  " + "  ".join(cols)
            )
            terminalreporter.write_line("")
        else:
            # Current values with deltas (colored: green=improvement, red=regression)
            def delta_str(cur, base):
                return f"({_format_delta(cur, base)})"

            def colr(cur, base):
                return _delta_color(cur, base, use_color)

            cols = []
            if MEMTEST_AVAILABLE:
                cols.append(
                    _col(
                        _format_bytes(s.peak_bytes),
                        delta_str(s.peak_bytes, b.peak_bytes),
                        vw,
                        dw,
                        colr(s.peak_bytes, b.peak_bytes),
                    )
                )
                cols.append(
                    _col(
                        _format_count(s.total_allocations),
                        delta_str(s.total_allocations, b.total_allocations),
                        vw,
                        dw,
                        colr(s.total_allocations, b.total_allocations),
                    )
                )
            cols.append(
                _col(
                    _format_iops(s.read_iops),
                    delta_str(s.read_iops, b.read_iops),
                    vw,
                    dw,
                    colr(s.read_iops, b.read_iops),
                )
            )
            cols.append(
                _col(
                    _format_bytes(s.read_bytes),
                    delta_str(s.read_bytes, b.read_bytes),
                    vw,
                    dw,
                    colr(s.read_bytes, b.read_bytes),
                )
            )
            terminalreporter.write_line(
                f"{result.name:<{name_width}}  " + "  ".join(cols)
            )

            # Baseline values line (empty delta for alignment)
            vs_cols = []
            if MEMTEST_AVAILABLE:
                vs_cols.append(_col(_format_bytes(b.peak_bytes), "", vw, dw))
                vs_cols.append(_col(_format_count(b.total_allocations), "", vw, dw))
            vs_cols.append(_col(_format_iops(b.read_iops), "", vw, dw))
            vs_cols.append(_col(_format_bytes(b.read_bytes), "", vw, dw))
            terminalreporter.write_line(f"{'vs':>{name_width}}  " + "  ".join(vs_cols))
            terminalreporter.write_line("")


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    """Print benchmark statistics summary at the end of the test run."""
    if not _benchmark_results:
        return

    terminalreporter.write_sep("=", "IO/Memory Benchmark Statistics")

    compare_name = config.getoption("--iom-compare")
    baseline = None
    if compare_name:
        baseline = _load_baseline(compare_name)
        if baseline is None:
            terminalreporter.write_line(
                f"WARNING: Baseline '{compare_name}' not found. "
                f"Available baselines in {BENCHMARKS_DIR}/:"
            )
            if BENCHMARKS_DIR.exists():
                for f in sorted(BENCHMARKS_DIR.glob("*.json")):
                    terminalreporter.write_line(f"  - {f.stem}")
            terminalreporter.write_line("")

    if baseline:
        _print_summary_with_compare(terminalreporter, _benchmark_results, baseline)
    else:
        _print_summary_no_compare(terminalreporter, _benchmark_results)

    if not MEMTEST_AVAILABLE:
        terminalreporter.write_line(
            "Note: Memory tracking not available. "
            "Run with LD_PRELOAD=$(lance-memtest) to enable."
        )

    terminalreporter.write_line("")


def pytest_sessionfinish(session, exitstatus):
    """Write benchmark results to JSON file if requested."""
    if not _benchmark_results:
        return

    # Handle --iom-save
    save_name = session.config.getoption("--iom-save")
    if save_name:
        path = _save_baseline(save_name, _benchmark_results)
        print(f"\nBenchmark baseline saved to: {path}")

    # Handle --benchmark-stats-json (Bencher Metric Format)
    output_path = session.config.getoption("--benchmark-stats-json")
    if not output_path:
        return

    bmf_output = {}
    for result in _benchmark_results:
        s = result.stats
        bmf_output[result.name] = {
            "read_iops": {"value": s.read_iops},
            "read_bytes": {"value": s.read_bytes},
            "write_iops": {"value": s.write_iops},
            "write_bytes": {"value": s.write_bytes},
        }
        if MEMTEST_AVAILABLE:
            bmf_output[result.name]["peak_memory_bytes"] = {"value": s.peak_bytes}
            bmf_output[result.name]["total_allocations"] = {
                "value": s.total_allocations
            }

    with open(output_path, "w") as f:
        json.dump(bmf_output, f, indent=2)
