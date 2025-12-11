# CI Benchmarks

This directory contains benchmarks that run in CI and report results to [bencher.dev](https://bencher.dev).

## Structure

```
ci_benchmarks/
├── benchmarks/          # Benchmark tests
│   ├── test_scan.py
│   ├── test_search.py
│   └── test_random_access.py
├── datagen/             # Dataset generation scripts
│   ├── gen_all.py       # Generate all datasets
│   ├── basic.py         # 10M row dataset
│   └── lineitems.py     # TPC-H lineitem dataset
├── benchmark.py         # IO/memory benchmark infrastructure
├── conftest.py          # Pytest configuration
└── datasets.py          # Dataset URI resolver (local vs GCS)
```

## Running Benchmarks Locally

### 1. Generate test datasets

```bash
python python/ci_benchmarks/datagen/gen_all.py
```

This creates datasets in `~/lance-benchmarks-ci-datasets/`.

### 2. Run pytest-benchmark tests

```bash
pytest python/ci_benchmarks/ --benchmark-only
```

To save timing results as JSON:

```bash
pytest python/ci_benchmarks/ --benchmark-json results.json
```

## IO/Memory Benchmarks

The `io_memory_benchmark` marker provides benchmarks that track both IO statistics
and memory allocations during the benchmark execution (not setup/teardown).

### Writing IO/Memory Benchmarks

```python
@pytest.mark.io_memory_benchmark()
def test_full_scan(io_mem_benchmark):
    dataset_uri = get_dataset_uri("basic")
    ds = lance.dataset(dataset_uri)

    def bench(dataset):
        dataset.to_table()

    io_mem_benchmark(bench, ds)
```

The `io_mem_benchmark` fixture:
- Runs an optional warmup iteration (not measured)
- Tracks IO stats via `dataset.io_stats_incremental()`
- Optionally tracks memory via `lance-memtest` if preloaded

### Running IO/Memory Benchmarks

Without memory tracking:
```bash
pytest python/ci_benchmarks/benchmarks/test_search.py::test_io_mem_basic_btree_search -v
```

With memory tracking (Linux only):
```bash
LD_PRELOAD=$(lance-memtest) pytest python/ci_benchmarks/benchmarks/test_search.py::test_io_mem_basic_btree_search -v
```

### Output

Terminal output shows a summary table:
```
======================== IO/Memory Benchmark Statistics ========================
Test                                     Peak Mem      Allocs   Read IOPS    Read Bytes
---------------------------------------------------------------------------------------
test_io_mem_basic_btree_search[...]        3.6 MB     135,387           2        1.8 MB
```

To save results as JSON (Bencher Metric Format):
```bash
pytest ... --benchmark-stats-json stats.json
```

## Uploading to Bencher

```bash
# Upload timing results (from pytest-benchmark)
bencher run --adapter python_pytest --file timing_results.json

# Upload IO/memory stats
bencher run --adapter json --file stats.json
```
