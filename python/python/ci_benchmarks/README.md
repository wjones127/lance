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
└── datasets.py          # Dataset URI resolver (local vs GCS)
```

## Running Benchmarks Locally

### 1. Generate test datasets

```bash
python python/ci_benchmarks/datagen/gen_all.py
```

This creates datasets in `~/lance-benchmarks-ci-datasets/`.

### 2. Run benchmarks

```bash
pytest python/ci_benchmarks/ --benchmark-only
```

To save results as JSON:

```bash
pytest python/ci_benchmarks/ --benchmark-json results.json
```

## Running with Memory Tracking (Linux-only)

To track memory allocations during benchmarks, use the `lance-memtest` library with `LD_PRELOAD`.

### 1. Install lance-memtest

```bash
pip install lance-memtest
```

### 2. Run with memory tracking

```bash
LD_PRELOAD=$(lance-memtest) pytest python/ci_benchmarks/ \
    --benchmark-json timing_results.json \
    --memory-json memory_results.json
```

This produces:
- `timing_results.json` - Standard pytest-benchmark timing results
- `memory_results.json` - Memory stats in Bencher Metric Format (BMF)

### 3. Using memory_benchmark fixture

For benchmarks that need memory tracking, use the `memory_benchmark` fixture instead of `benchmark`:

```python
def test_full_scan(memory_benchmark, dataset):
    memory_benchmark(dataset.to_table)
```

When `LD_PRELOAD` is not set, `memory_benchmark` passes through to the regular `benchmark` fixture.

## Uploading to Bencher

```bash
# Upload timing results
bencher run --adapter python_pytest --file timing_results.json

# Upload memory results
bencher run --adapter json --file memory_results.json
```
