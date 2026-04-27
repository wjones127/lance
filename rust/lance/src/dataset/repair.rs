// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Detect and repair recoverable corruptions in a Lance dataset.
//!
//! Today the only issue detected is the fragment-reuse-index "straddle" case
//! introduced by the bug fixed in
//! <https://github.com/lance-format/lance/pull/6610>: a user index's
//! `fragment_bitmap` ends up containing only some of a rewrite group's
//! `old_frags`. Once committed, `Dataset::load_indices` panics on the
//! `.unwrap()` of `remap_fragment_bitmap`. The repair removes the
//! straddling old-fragment IDs from the affected bitmap; previously-indexed
//! rows that landed in the merged new fragment fall through to flat scan
//! until the next `optimize_indices`. No data is lost and no index is
//! retrained.

use std::fmt;

use lance_core::Result;
use lance_index::frag_reuse::FRAG_REUSE_INDEX_NAME;
use lance_index::is_system_index;
use lance_table::format::IndexMetadata;
use lance_table::io::manifest::read_manifest_indexes;
use roaring::RoaringBitmap;
use uuid::Uuid;

use crate::Dataset;
use crate::dataset::ManifestWriteConfig;
use crate::dataset::transaction::{Operation, Transaction};
use crate::index::frag_reuse::load_frag_reuse_index_details;

/// A recoverable corruption discovered in a dataset.
#[derive(Debug, Clone, PartialEq)]
pub enum DatasetIssue {
    /// A user index segment's `fragment_bitmap` contains some, but not all,
    /// of the `old_frags` of a fragment-reuse rewrite group. See PR #6610.
    FriIndexBitmapStraddle {
        index_name: String,
        index_uuid: Uuid,
        fri_dataset_version: u64,
        group_old_frags: Vec<u64>,
        indexed_subset: Vec<u64>,
    },
}

impl fmt::Display for DatasetIssue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FriIndexBitmapStraddle {
                index_name,
                index_uuid,
                fri_dataset_version,
                group_old_frags,
                indexed_subset,
            } => write!(
                f,
                "FRI bitmap straddle: index '{}' ({}) has bitmap covering {:?} \
                 but not all of rewrite group {:?} from FRI version {}",
                index_name, index_uuid, indexed_subset, group_old_frags, fri_dataset_version
            ),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RepairOptions {
    /// When true, detect issues but do not modify the dataset.
    pub dry_run: bool,
}

#[derive(Debug, Default, Clone)]
pub struct RepairReport {
    /// Every issue detected on the dataset.
    pub issues: Vec<DatasetIssue>,
    /// Issues this call repaired. Subset of `issues`. Empty when `dry_run`.
    pub repaired: Vec<DatasetIssue>,
}

/// Scan the dataset's manifest for recoverable corruptions without going
/// through `Dataset::load_indices` (which would panic on the FRI straddle
/// case). Safe to call on healthy tables — returns an empty vector.
pub async fn detect_issues(dataset: &Dataset) -> Result<Vec<DatasetIssue>> {
    let raw_indices = read_manifest_indexes(
        &dataset.object_store,
        &dataset.manifest_location,
        &dataset.manifest,
    )
    .await?;
    let Some(fri_meta) = raw_indices.iter().find(|i| i.name == FRAG_REUSE_INDEX_NAME) else {
        return Ok(Vec::new());
    };
    let fri = load_frag_reuse_index_details(dataset, fri_meta).await?;

    let mut issues = Vec::new();
    for index in raw_indices.iter() {
        if is_system_index(index) {
            continue;
        }
        let Some(bitmap) = index.fragment_bitmap.as_ref() else {
            continue;
        };
        for version in fri.versions.iter() {
            for group in version.groups.iter() {
                let old_ids: Vec<u64> = group.old_frags.iter().map(|f| f.id).collect();
                let indexed: Vec<u64> = old_ids
                    .iter()
                    .copied()
                    .filter(|id| bitmap.contains(*id as u32))
                    .collect();
                if !indexed.is_empty() && indexed.len() < old_ids.len() {
                    issues.push(DatasetIssue::FriIndexBitmapStraddle {
                        index_name: index.name.clone(),
                        index_uuid: index.uuid,
                        fri_dataset_version: version.dataset_version,
                        group_old_frags: old_ids,
                        indexed_subset: indexed,
                    });
                }
            }
        }
    }
    Ok(issues)
}

/// Detect and repair recoverable corruptions. Idempotent on healthy tables.
///
/// Repair semantics for `FriIndexBitmapStraddle`: remove the straddling old
/// fragment IDs from the affected segment's `fragment_bitmap`. Rows in the
/// merged new fragment will be scanned without index assistance until the
/// next `optimize_indices`.
pub async fn repair(dataset: &mut Dataset, options: &RepairOptions) -> Result<RepairReport> {
    let issues = detect_issues(dataset).await?;
    if issues.is_empty() || options.dry_run {
        return Ok(RepairReport {
            issues,
            repaired: Vec::new(),
        });
    }

    // Group repairs by index uuid and compute the new bitmap once per
    // affected segment.
    let raw_indices = read_manifest_indexes(
        &dataset.object_store,
        &dataset.manifest_location,
        &dataset.manifest,
    )
    .await?;

    let mut new_indices: Vec<IndexMetadata> = Vec::new();
    let mut removed_indices: Vec<IndexMetadata> = Vec::new();

    for index in raw_indices.iter() {
        if is_system_index(index) {
            continue;
        }
        let Some(bitmap) = index.fragment_bitmap.as_ref() else {
            continue;
        };
        let mut repaired_bitmap: RoaringBitmap = bitmap.clone();
        let mut changed = false;
        for issue in issues.iter() {
            let DatasetIssue::FriIndexBitmapStraddle {
                index_uuid,
                indexed_subset,
                ..
            } = issue;
            if *index_uuid != index.uuid {
                continue;
            }
            for id in indexed_subset {
                if repaired_bitmap.remove(*id as u32) {
                    changed = true;
                }
            }
        }
        if changed {
            let mut fixed = index.clone();
            fixed.fragment_bitmap = Some(repaired_bitmap);
            new_indices.push(fixed);
            removed_indices.push(index.clone());
        }
    }

    if new_indices.is_empty() {
        return Ok(RepairReport {
            issues,
            repaired: Vec::new(),
        });
    }

    commit_repaired_indices(dataset, raw_indices, new_indices, removed_indices).await?;

    // Reopen the dataset so subsequent calls see the repaired manifest.
    *dataset = Dataset::open(dataset.uri()).await?;

    Ok(RepairReport {
        issues: issues.clone(),
        repaired: issues,
    })
}

/// Commit a `CreateIndex { new_indices, removed_indices }` transaction
/// without going through `commit_transaction`. The latter calls
/// `Dataset::load_indices` to seed the manifest builder, which panics on
/// the corrupt FRI/index state we are trying to fix. Here we feed in the
/// raw indices read directly from the current manifest instead.
async fn commit_repaired_indices(
    dataset: &Dataset,
    raw_indices: Vec<IndexMetadata>,
    new_indices: Vec<IndexMetadata>,
    removed_indices: Vec<IndexMetadata>,
) -> Result<()> {
    let read_version = dataset.manifest.version;
    let target_version = read_version + 1;

    let transaction = Transaction::new(
        read_version,
        Operation::CreateIndex {
            new_indices,
            removed_indices,
        },
        None,
    );

    let write_config = ManifestWriteConfig::default();
    let (mut manifest, indices) = transaction.build_manifest(
        Some(dataset.manifest.as_ref()),
        raw_indices,
        "",
        &write_config,
    )?;
    manifest.version = target_version;

    let result = crate::dataset::write_manifest_file(
        &dataset.object_store,
        dataset.commit_handler.as_ref(),
        &dataset.base,
        &mut manifest,
        if indices.is_empty() {
            None
        } else {
            Some(indices)
        },
        &write_config,
        dataset.manifest_location.naming_scheme,
        Some(&transaction),
    )
    .await;

    result.map(|_| ()).map_err(lance_core::Error::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::DatasetIndexExt;
    use crate::utils::test::copy_test_data_to_tmp;
    use lance_core::utils::tempfile::TempStrDir;

    /// Copies the checked-in pre-#6610 corrupt dataset and returns
    /// `(temp_dir, file_uri)`. Used by the straddle-specific tests so they
    /// keep working after PR #6610 is rebased in (the in-tree
    /// `make_fri_straddle_dataset` helper requires the buggy conflict
    /// resolver to commit successfully).
    fn corrupt_fixture() -> (lance_core::utils::tempfile::TempDir, String) {
        let tmp = copy_test_data_to_tmp("fri_straddle_pre_6610/fri_straddle_dataset").unwrap();
        let uri = format!("file://{}", tmp.std_path().display());
        (tmp, uri)
    }

    #[tokio::test]
    async fn test_detect_issues_finds_straddle() {
        let (_tmp, uri) = corrupt_fixture();
        let dataset = Dataset::open(&uri).await.unwrap();

        let issues = detect_issues(&dataset).await.unwrap();
        assert!(!issues.is_empty(), "fixture should reproduce a straddle");
        for issue in &issues {
            let DatasetIssue::FriIndexBitmapStraddle {
                indexed_subset,
                group_old_frags,
                ..
            } = issue;
            assert!(!indexed_subset.is_empty());
            assert!(indexed_subset.len() < group_old_frags.len());
        }
    }

    #[tokio::test]
    async fn test_detect_issues_clean_table_is_empty() {
        use crate::dataset::WriteParams;
        use arrow_array::types::Float32Type;
        use lance_datagen::Dimension;
        use lance_datagen::{BatchCount, RowCount, array, gen_batch};

        let tmp = TempStrDir::default();
        let uri = format!("file://{}", tmp.as_str());
        let reader = gen_batch()
            .col("vec", array::rand_vec::<Float32Type>(Dimension::from(8)))
            .into_reader_rows(RowCount::from(64), BatchCount::from(1));
        Dataset::write(reader, &uri, Some(WriteParams::default()))
            .await
            .unwrap();
        let ds = Dataset::open(&uri).await.unwrap();
        assert!(detect_issues(&ds).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_validate_surfaces_straddle_as_error() {
        let (_tmp, uri) = corrupt_fixture();
        let dataset = Dataset::open(&uri).await.unwrap();

        let err = dataset
            .validate()
            .await
            .expect_err("validate should surface the straddle as an error");
        let msg = format!("{err}");
        assert!(
            msg.contains("FRI bitmap straddle"),
            "error message missing issue description: {msg}"
        );
        assert!(
            msg.contains("Dataset::repair"),
            "error message should point at the repair API: {msg}"
        );
    }

    #[tokio::test]
    async fn test_repair_fixes_straddle() {
        let (_tmp, uri) = corrupt_fixture();

        let mut dataset = Dataset::open(&uri).await.unwrap();
        let report = repair(&mut dataset, &RepairOptions::default())
            .await
            .unwrap();
        assert!(!report.issues.is_empty());
        assert_eq!(report.repaired.len(), report.issues.len());

        // load_indices must no longer panic and validate must succeed.
        let dataset = Dataset::open(&uri).await.unwrap();
        let _ = dataset.load_indices().await.unwrap();
        dataset.validate().await.unwrap();

        // No remaining straddles.
        assert!(detect_issues(&dataset).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_repair_dry_run_no_mutation() {
        let (_tmp, uri) = corrupt_fixture();

        let mut dataset = Dataset::open(&uri).await.unwrap();
        let version_before = dataset.manifest.version;
        let report = repair(&mut dataset, &RepairOptions { dry_run: true })
            .await
            .unwrap();
        assert!(!report.issues.is_empty());
        assert!(report.repaired.is_empty());

        let dataset = Dataset::open(&uri).await.unwrap();
        assert_eq!(dataset.manifest.version, version_before);
        assert!(!detect_issues(&dataset).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_repair_idempotent_on_healthy_table() {
        use crate::dataset::WriteParams;
        use arrow_array::types::Float32Type;
        use lance_datagen::Dimension;
        use lance_datagen::{BatchCount, RowCount, array, gen_batch};

        let tmp = TempStrDir::default();
        let uri = format!("file://{}", tmp.as_str());
        let reader = gen_batch()
            .col("vec", array::rand_vec::<Float32Type>(Dimension::from(8)))
            .into_reader_rows(RowCount::from(64), BatchCount::from(1));
        Dataset::write(reader, &uri, Some(WriteParams::default()))
            .await
            .unwrap();

        let mut dataset = Dataset::open(&uri).await.unwrap();
        let version_before = dataset.manifest.version;
        let report = repair(&mut dataset, &RepairOptions::default())
            .await
            .unwrap();
        assert!(report.issues.is_empty());
        assert!(report.repaired.is_empty());
        assert_eq!(
            Dataset::open(&uri).await.unwrap().manifest.version,
            version_before
        );
    }
}
