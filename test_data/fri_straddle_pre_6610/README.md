# `fri_straddle_pre_6610`

A dataset corrupted by the bug fixed in
[lance-format/lance#6610](https://github.com/lance-format/lance/pull/6610):
a deferred-remap compaction commits concurrently with `optimize_indices`
on a stale handle, leaving a user index whose `fragment_bitmap` straddles
a fragment-reuse rewrite group. `Dataset::list_indices()` panics with
"The compaction plan included a rewrite group that was a split of indexed
and non-indexed data".

Used by `dataset::repair` Rust tests to verify detection,
`Dataset::validate()` error reporting, and `Dataset::repair()` against a
real on-disk corrupt manifest.

## Regenerating

The reproduction is deterministic in Rust because we can drive
`plan_compaction` / `CompactionTask::execute` / `commit_compaction`
directly with `defer_index_remap=true` and interleave `optimize_indices`
between them. Python's `Compaction.commit` hardcodes
`CompactionOptions::default()` (no defer-remap), so a Python datagen
cannot reach this path without a tight commit race.

The generator must be run on a Lance build that does **not** contain PR
\#6610. On a fixed build the conflict resolver rejects the rewrite as a
retryable conflict and the generator fails loudly. To regenerate:

```bash
# from a checkout that pre-dates PR #6610 (e.g. tag v4.0.1)
cargo run -p lance-examples --example fri_straddle_datagen -- \
    test_data/fri_straddle_pre_6610/fri_straddle_dataset
```

Source: `rust/examples/src/fri_straddle_datagen.rs`.
