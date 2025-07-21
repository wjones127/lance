# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Lance is a modern columnar data format optimized for machine learning workflows, built primarily in Rust with Python and Java bindings. It's designed as a 100x faster alternative to Parquet for random access operations, with native vector indexing and versioning support.

## Development tips

Code standards:
* Be mindful of memory use:
  * When dealing with streams of `RecordBatch`, avoid collecting all data into
    memory whenever possible.
  * Use `RoaringBitmap` instead `HashSet<u32>`.

Tests:
* When writing unit tests, prefer using the `memory://` URI instead of creating
  a temporary directory.
* Use rstest to generate parameterized tests to cover more cases with fewer lines
  of code.
    * Use syntax `#[case::{name}(...)]` to provide human-readable names for each case.
* For backwards compatibility, use the `test_data` directory to check in datasets
  written with older library version.
    * Check in a `datagen.py` that creates the test data. It should assert the
      version of Lance used as part of the script.
    * Use `pip install pylance=={version}` and then run `python datagen.py` to
      create the dataset. The data files should be checked into git.
    * Use `copy_test_data_to_tmp` to read this data in Lance

## Lance format crash course

Lance is a table format. It stores tabular data as a set of immutable files.

The full state of the table is described in a manifest file. There is a linear
sequence of manifest files, which keep track of the state of the table over time.
This is a multi-version concurrency control style of table state.

Lance implements updates as delete and then insert.

Every row implicitly has a `_rowid` (row id) and `_rowaddr` (row address) associated with it.
|------|-------|------|----------|------------|
|   id |   str |  val | (_rowid) | (_rowaddr) |
|------|-------|------|----------|------------|

