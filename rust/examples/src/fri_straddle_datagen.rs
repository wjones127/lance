//! Generator for the `fri_straddle_pre_6610` test fixture.
//!
//! Reproduces the corrupt dataset state caused by the bug fixed in
//! <https://github.com/lance-format/lance/pull/6610>: a deferred-remap
//! compaction commits concurrently with `optimize_indices` against an older
//! dataset version, leaving a user index whose `fragment_bitmap` straddles a
//! fragment-reuse rewrite group. After this state is reached,
//! `Dataset::list_indices` panics.
//!
//! The reproduction uses the public Rust APIs (`plan_compaction`,
//! `CompactionTask::execute`, `optimize_indices`, `commit_compaction`) and is
//! therefore deterministic — unlike a Python script driving
//! `compact_files(defer_index_remap=True)` against a concurrent
//! `optimize_indices`, which depends on a tight commit race that doesn't land
//! reliably.
//!
//! ## Usage
//!
//! Must be run on a Lance build that does *not* contain PR #6610 (the
//! conflict resolver still has the buggy `(None, Some(_)) => Ok(())` arm in
//! `rust/lance/src/io/commit/conflict_resolver.rs`). On a fixed build the
//! commit will be rejected as a retryable conflict and the generator will
//! return an error. To regenerate the fixture in the future, check out a
//! pre-#6610 commit (e.g. tag `v4.0.1`) and run:
//!
//! ```bash
//! cargo run -p lance-examples --example fri_straddle_datagen -- \
//!     test_data/fri_straddle_pre_6610/fri_straddle_dataset
//! ```

use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::Float32Type;
use clap::Parser;
use lance::Dataset;
use lance::dataset::index::DatasetIndexRemapperOptions;
use lance::dataset::optimize::{
    CompactionOptions, RewriteResult, commit_compaction, plan_compaction,
};
use lance::dataset::{WriteMode, WriteParams};
use lance::index::DatasetIndexExt;
use lance::index::vector::VectorIndexParams;
use lance_datagen::{BatchCount, Dimension, RowCount, array, gen_batch};
use lance_index::IndexType;
use lance_linalg::distance::MetricType;

#[derive(Parser, Debug)]
#[command(about = "Generate the fri_straddle_pre_6610 test fixture")]
struct Args {
    /// Path to write the generated dataset. Existing contents are removed.
    output: PathBuf,
}

async fn append_fragment(uri: &str, rows: u64) -> Dataset {
    let reader = gen_batch()
        .col("vec", array::rand_vec::<Float32Type>(Dimension::from(16)))
        .into_reader_rows(RowCount::from(rows), BatchCount::from(1));
    Dataset::write(
        reader,
        uri,
        Some(WriteParams {
            max_rows_per_file: rows as usize,
            mode: WriteMode::Append,
            ..Default::default()
        }),
    )
    .await
    .unwrap()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    if args.output.exists() {
        std::fs::remove_dir_all(&args.output)?;
    }
    std::fs::create_dir_all(&args.output)?;
    let uri = format!("file://{}", args.output.canonicalize()?.display());

    // frag0: indexed.
    let reader = gen_batch()
        .col("vec", array::rand_vec::<Float32Type>(Dimension::from(16)))
        .into_reader_rows(RowCount::from(256), BatchCount::from(1));
    let mut dataset = Dataset::write(
        reader,
        &uri,
        Some(WriteParams {
            max_rows_per_file: 256,
            mode: WriteMode::Overwrite,
            ..Default::default()
        }),
    )
    .await?;
    let index_params = VectorIndexParams::ivf_pq(2, 8, 2, MetricType::L2, 50);
    dataset
        .create_index(&["vec"], IndexType::Vector, None, &index_params, true)
        .await?;

    // Append frag1, snapshot the version before frag2 lands.
    dataset = append_fragment(&uri, 64).await;
    let mut stale = dataset.clone();

    // Append frag2 on the up-to-date handle.
    dataset = append_fragment(&uri, 64).await;

    // Plan + execute deferred-remap compaction of frag1+frag2 against the
    // up-to-date dataset, but do *not* commit yet.
    let options = CompactionOptions {
        defer_index_remap: true,
        ..Default::default()
    };
    let plan = plan_compaction(&dataset, &options).await?;
    if plan.tasks.is_empty() {
        return Err("plan_compaction produced no tasks; cannot reproduce the bug".into());
    }
    let snapshot = dataset.clone();
    let mut completed: Vec<RewriteResult> = Vec::new();
    for task in plan.compaction_tasks() {
        completed.push(task.execute(&snapshot).await?);
    }

    // Stale optimize_indices commits a CreateIndex covering frag1 only —
    // frag2 didn't exist at this dataset version. This is the commit that
    // PR #6610 should have rejected against the still-uncommitted Rewrite,
    // but doesn't on pre-fix builds.
    stale
        .optimize_indices(&lance_index::optimize::OptimizeOptions::append())
        .await?;

    // Commit the rewrite. On pre-#6610 builds the (None, Some(_)) arm of the
    // conflict resolver returns COMPATIBLE and the corrupt state is written
    // to disk. On a fixed build this will return RetryableCommitConflict and
    // the regenerator will fail loudly — which is the desired behaviour.
    commit_compaction(
        &mut dataset,
        completed,
        Arc::new(DatasetIndexRemapperOptions::default()),
        &options,
    )
    .await
    .map_err(|e| {
        format!(
            "commit_compaction failed: {e}. \
             This generator must be run on a Lance build without PR #6610 applied."
        )
    })?;

    println!(
        "Generated fri_straddle fixture at {}",
        args.output.display()
    );
    Ok(())
}
