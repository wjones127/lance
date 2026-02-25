// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Dataset file inspection APIs.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{
    Int64Builder, StringBuilder, StringDictionaryBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::types::Int32Type;
use arrow_array::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use either::Either;
use futures::{StreamExt, TryStreamExt};
use lance_table::utils::LanceIteratorExtension;
use object_store::path::Path;
use uuid::Uuid;

use crate::dataset::files::arrow::{TrackedFileBatch, TRACKED_FILES_SCHEMA};
use crate::dataset::files::file_types::FileType;
use crate::dataset::{DATA_DIR, INDICES_DIR, TRANSACTIONS_DIR};
use crate::Dataset;
use lance_core::Result;
use lance_table::io::deletion::relative_deletion_file_path;
use lance_table::io::manifest::{read_manifest, read_manifest_indexes};

mod arrow;
mod file_types;

const BATCH_SIZE: usize = 4096;

fn remove_prefix(path: &Path, prefix: &Path) -> Path {
    match path.prefix_match(prefix) {
        Some(parts) => Path::from_iter(parts),
        None => path.clone(),
    }
}

/// A single row destined for the `tracked_files` output.
struct FileRow<'a> {
    version: u64,
    base_uri: Cow<'a, str>,
    path: Cow<'a, str>,
    file_type: FileType,
}

fn manifest_file_rows<'a>(
    manifest: &'a lance_table::format::Manifest,
    base_uri: &'a str,
    manifest_path: &'a str,
) -> Box<dyn ExactSizeIterator<Item = FileRow<'a>> + Send + 'a> {
    let mut files = 1;
    let manifest_row = FileRow {
        version: manifest.version,
        base_uri: Cow::Borrowed(base_uri),
        path: Cow::Borrowed(manifest_path),
        file_type: FileType::Manifest,
    };
    let iter = std::iter::once(manifest_row);

    let iter = if let Some(txn_file) = &manifest.transaction_file {
        files += 1;
        let txn_row = FileRow {
            version: manifest.version,
            base_uri: Cow::Borrowed(base_uri),
            path: Cow::Owned(format!("{}/{}", TRANSACTIONS_DIR, txn_file)),
            file_type: FileType::TransactionFile,
        };
        Either::Left(iter.chain(std::iter::once(txn_row)))
    } else {
        Either::Right(iter)
    };

    for fragment in manifest.fragments.iter() {
        files += fragment.files.len();

        if fragment.deletion_file.is_some() {
            files += 1;
        }
    }

    let data_files = manifest.fragments.iter().flat_map(move |fragment| {
        let base_uri = fragment
            .files
            .iter()
            .find_map(|f| {
                f.base_id
                    .and_then(|id| manifest.base_paths.get(&id).map(|bp| bp.path.as_str()))
            })
            .unwrap_or(base_uri);
        let data_rows = fragment.files.iter().map(move |data_file| FileRow {
            version: manifest.version,
            base_uri: Cow::Borrowed(base_uri),
            path: Cow::Owned(format!("{}/{}", DATA_DIR, data_file.path)),
            file_type: FileType::DataFile,
        });
        data_rows
    });

    let deletion_files = manifest.fragments.iter().filter_map(|fragment| {
        fragment.deletion_file.as_ref().map(|del_file| FileRow {
            version: manifest.version,
            base_uri: Cow::Borrowed(base_uri),
            path: Cow::Owned(relative_deletion_file_path(fragment.id, del_file)),
            file_type: FileType::DeletionFile,
        })
    });

    Box::new(
        iter.chain(data_files)
            .chain(deletion_files)
            .exact_size(files),
    )
}

fn manifest_file_batches<'a>(
    manifest: &'a lance_table::format::Manifest,
    base_uri: &'a str,
    manifest_path: &'a str,
) -> Box<dyn ExactSizeIterator<Item = Result<RecordBatch>> + Send + 'a> {
    let mut builder = TrackedFileBatch::with_capacity(BATCH_SIZE);

    let mut iter = manifest_file_rows(manifest, base_uri, manifest_path);
    let size = iter.len().div_ceil(BATCH_SIZE);

    let mut flushed = false;
    Box::new(
        std::iter::from_fn(move || {
            if flushed {
                return None;
            }
            while let Some(row) = iter.next() {
                builder.append(&row);
                if builder.len() == BATCH_SIZE {
                    let next_size = iter.len().div_ceil(BATCH_SIZE);
                    let old_builder =
                        std::mem::replace(&mut builder, TrackedFileBatch::with_capacity(next_size));
                    return Some(old_builder.finish());
                }
            }
            // Flush the remaining partial batch.
            flushed = true;
            if builder.len() != 0 {
                let partial = std::mem::replace(&mut builder, TrackedFileBatch::with_capacity(0));
                Some(partial.finish())
            } else {
                None
            }
        })
        .exact_size(size),
    )
}

async fn get_index_files(
    uuids: impl IntoIterator<Item = Uuid>,
    base: &Path,
    object_store: &lance_io::object_store::ObjectStore,
    cache: &mut HashMap<Uuid, Vec<object_store::ObjectMeta>>,
) -> Result<Vec<Path>> {
    let uuids: Vec<Uuid> = uuids.into_iter().collect();

    // Phase 1: list uncached UUID directories concurrently.
    let uncached: Vec<Uuid> = uuids
        .iter()
        .filter(|uuid| !cache.contains_key(*uuid))
        .copied()
        .collect();
    if !uncached.is_empty() {
        let parallelism = object_store.io_parallelism();
        // Clone for use in async move closures (ObjectStore is Arc-backed).
        let base_owned = base.clone();
        let os = object_store.clone();
        let new_entries: Vec<(Uuid, Vec<object_store::ObjectMeta>)> =
            futures::stream::iter(uncached)
                .map(|uuid| {
                    let base = base_owned.clone();
                    let os = os.clone();
                    async move {
                        let prefix = base.child(INDICES_DIR).child(uuid.to_string());
                        let files: Vec<object_store::ObjectMeta> =
                            os.list(Some(prefix)).try_collect().await?;
                        lance_core::Result::Ok((uuid, files))
                    }
                })
                .buffer_unordered(parallelism)
                .try_collect()
                .await?;

        // Phase 2: insert results into cache (serial, no contention).
        cache.extend(new_entries);
    }

    // Phase 3: collect paths for the requested UUIDs in order.
    let mut paths = Vec::new();
    for uuid in &uuids {
        paths.extend(
            cache[uuid]
                .iter()
                .map(|meta| remove_prefix(&meta.location, base)),
        );
    }
    Ok(paths)
}

async fn index_file_batch(version: u64, base_uri: &str, paths: &[Path]) -> Result<RecordBatch> {
    let mut builder = TrackedFileBatch::with_capacity(paths.len());
    for path in paths {
        builder.append(&FileRow {
            version,
            base_uri: Cow::Borrowed(base_uri),
            path: Cow::Owned(path.to_string()),
            file_type: FileType::IndexFile,
        });
    }
    builder.finish()
}

impl Dataset {
    /// Returns one row per (version, file) for every file referenced in any manifest.
    ///
    /// Each row contains the manifest version, the storage root URI, the file path
    /// relative to that URI, and the file type.
    ///
    /// # Schema
    ///
    /// | Column     | Type                              | Notes |
    /// |------------|-----------------------------------|-------|
    /// | `version`  | `Int64` (non-null)                | Manifest version number |
    /// | `base_uri` | `Dictionary(Int32, Utf8)` (non-null) | Storage root for this file |
    /// | `path`     | `Utf8` (non-null)                 | Relative to `base_uri` |
    /// | `type`     | `Dictionary(Int32, Utf8)` (non-null) | One of: `data file`, `manifest`, `deletion file`, `transaction file`, `index file` |
    ///
    /// Output order is non-deterministic.
    pub async fn tracked_files(&self) -> Result<SendableRecordBatchStream> {
        let base = self.base.clone();
        let uri = self.uri().to_string();
        let object_store = self.object_store.clone();
        let commit_handler = self.commit_handler.clone();

        let (tx, rx) = tokio::sync::mpsc::channel::<datafusion::error::Result<RecordBatch>>(4);

        let _task = tokio::spawn(async move {
            let result: lance_core::Result<()> = async {
                let mut manifest_locations =
                    commit_handler.list_manifest_locations(&base, &object_store, false);

                // uuid -> files under that index directory.
                // Cached to avoid re-listing the same UUID directory across manifest versions.
                let mut uuid_cache: HashMap<Uuid, Vec<object_store::ObjectMeta>> = HashMap::new();

                while let Some(loc) = manifest_locations.next().await {
                    let loc = loc?;
                    let manifest = read_manifest(&object_store, &loc.path, loc.size).await?;
                    let manifest_path = remove_prefix(&loc.path, &base);
                    let batches = manifest_file_batches(&manifest, &uri, manifest_path.as_ref());
                    for batch_result in batches {
                        let df_result =
                            batch_result.map_err(datafusion::error::DataFusionError::from);
                        if tx.send(df_result).await.is_err() {
                            return Ok(());
                        }
                    }

                    let indexes = read_manifest_indexes(&object_store, &loc, &manifest).await?;
                    let uuids: Vec<Uuid> = indexes.iter().map(|idx| idx.uuid).collect();
                    let index_paths =
                        get_index_files(uuids, &base, &object_store, &mut uuid_cache).await?;
                    if !index_paths.is_empty() {
                        let batch = index_file_batch(manifest.version, &uri, &index_paths).await?;
                        if tx.send(Ok(batch)).await.is_err() {
                            return Ok(());
                        }
                    }
                }

                Ok(())
            }
            .await;

            if let Err(e) = result {
                // Best-effort: send the error to the stream consumer. If the
                // receiver is already gone we silently discard it.
                let _ = tx
                    .send(Err(datafusion::error::DataFusionError::from(e)))
                    .await;
            }
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            TRACKED_FILES_SCHEMA.clone(),
            stream,
        )))
    }

    /// Returns one row per file that physically exists at the dataset's base URI.
    ///
    /// This scans the primary object store root only. Additional `base_paths`
    /// entries in the manifest (for externally-located data files) are not
    /// scanned by this method.
    ///
    /// # Schema
    ///
    /// | Column          | Type                                       | Notes |
    /// |-----------------|--------------------------------------------|-------|
    /// | `base_uri`      | `Dictionary(Int32, Utf8)` (non-null)       | Storage root |
    /// | `path`          | `Utf8` (non-null)                          | Relative to `base_uri` |
    /// | `size_bytes`    | `Int64` (non-null)                         | File size in bytes |
    /// | `last_modified` | `Timestamp(Microsecond, "UTC")` (non-null) | Last modification time |
    pub async fn all_files(&self) -> SendableRecordBatchStream {
        let base = self.base.clone();
        let uri = self.uri().to_string();
        let object_store = self.object_store.clone();

        let stream = object_store
            .list(Some(base.clone()))
            .try_chunks(4000)
            .map_err(|err| err.1)
            .and_then(
                move |chunk| match build_all_files_batch(&chunk, &base, &uri) {
                    Ok(batch) => futures::future::ok(batch),
                    Err(e) => futures::future::err(e),
                },
            )
            .map_err(datafusion::error::DataFusionError::from);

        Box::pin(RecordBatchStreamAdapter::new(
            arrow::ALL_FILES_SCHEMA.clone(),
            stream,
        ))
    }
}

fn build_all_files_batch(
    chunk: &[object_store::ObjectMeta],
    base: &Path,
    uri: &str,
) -> Result<RecordBatch> {
    let n = chunk.len();
    let mut base_uri_builder = StringDictionaryBuilder::<Int32Type>::with_capacity(n, 1, uri.len());
    let path_capacity = chunk.iter().map(|m| m.location.as_ref().len()).sum();
    let mut path_builder = StringBuilder::with_capacity(n, path_capacity);
    let mut size_builder = Int64Builder::with_capacity(n);
    let mut ts_builder = TimestampMicrosecondBuilder::with_capacity(n).with_timezone("UTC");

    for meta in chunk {
        let rel = remove_prefix(&meta.location, base);
        base_uri_builder.append_value(uri);
        path_builder.append_value(rel.as_ref());
        size_builder.append_value(meta.size as i64);
        ts_builder.append_value(meta.last_modified.timestamp_micros());
    }

    RecordBatch::try_new(
        arrow::ALL_FILES_SCHEMA.clone(),
        vec![
            Arc::new(base_uri_builder.finish()),
            Arc::new(path_builder.finish()),
            Arc::new(size_builder.finish()),
            Arc::new(ts_builder.finish()),
        ],
    )
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::vector::VectorIndexParams;
    use crate::Dataset;
    use arrow_array::{Array, Int32Array, RecordBatchIterator, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
    use futures::TryStreamExt;
    use lance_index::{DatasetIndexExt, IndexType};
    use lance_linalg::distance::MetricType;
    use lance_testing::datagen::some_batch;
    use std::collections::HashSet;

    async fn collect_rows(stream: SendableRecordBatchStream) -> Vec<RecordBatch> {
        stream.try_collect::<Vec<_>>().await.unwrap()
    }

    fn count_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    fn dict_value_at(col: &dyn arrow_array::Array, i: usize) -> String {
        if let Some(dict) = col
            .as_any()
            .downcast_ref::<arrow_array::DictionaryArray<arrow_array::types::Int8Type>>()
        {
            let values = dict
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            values.value(dict.keys().value(i) as usize).to_string()
        } else if let Some(dict) = col
            .as_any()
            .downcast_ref::<arrow_array::DictionaryArray<arrow_array::types::Int32Type>>()
        {
            let values = dict
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            values.value(dict.keys().value(i) as usize).to_string()
        } else {
            panic!("expected a dictionary array with Int8 or Int32 keys");
        }
    }

    fn collect_column_values(batches: &[RecordBatch], col: &str) -> Vec<String> {
        batches
            .iter()
            .flat_map(|b| {
                let col = b.column_by_name(col).unwrap();
                (0..col.len()).map(|i| dict_value_at(col.as_ref(), i))
            })
            .collect()
    }

    fn make_simple_batch() -> impl arrow_array::RecordBatchReader {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        RecordBatchIterator::new(vec![Ok(batch)], schema)
    }

    #[tokio::test]
    async fn test_tracked_files_basic() {
        let uri = "memory://test_tracked_files_basic";

        // Create then append twice to get 3 manifest versions.
        let mut ds = Dataset::write(make_simple_batch(), uri, None)
            .await
            .unwrap();
        ds.append(make_simple_batch(), None).await.unwrap();
        ds.append(make_simple_batch(), None).await.unwrap();

        let stream = ds.tracked_files().await.unwrap();
        let schema = stream.schema();
        let batches = collect_rows(stream).await;

        // Schema is correct.
        assert_eq!(schema.field(0).name(), "version");
        assert_eq!(schema.field(1).name(), "base_uri");
        assert_eq!(schema.field(2).name(), "path");
        assert_eq!(schema.field(3).name(), "type");

        let n = count_rows(&batches);
        // At minimum: 3 manifests + 3 data files = 6 rows
        assert!(n >= 6, "expected at least 6 rows, got {n}");

        let types: HashSet<String> = collect_column_values(&batches, "type")
            .into_iter()
            .collect();
        assert!(types.contains("manifest"), "missing 'manifest' rows");
        assert!(types.contains("data file"), "missing 'data file' rows");
    }

    #[tokio::test]
    async fn test_tracked_files_deletion() {
        let uri = "memory://test_tracked_files_deletion";

        let mut ds = Dataset::write(make_simple_batch(), uri, None)
            .await
            .unwrap();
        ds.delete("id = 2").await.unwrap();

        let stream = ds.tracked_files().await.unwrap();
        let batches = collect_rows(stream).await;

        let types: HashSet<String> = collect_column_values(&batches, "type")
            .into_iter()
            .collect();
        assert!(
            types.contains("deletion file"),
            "missing 'deletion file' rows after delete; got types: {:?}",
            types
        );
    }

    #[tokio::test]
    async fn test_tracked_files_transaction() {
        let uri = "memory://test_tracked_files_transaction";

        // Normal writes record transaction files by default.
        let mut ds = Dataset::write(make_simple_batch(), uri, None)
            .await
            .unwrap();
        ds.append(make_simple_batch(), None).await.unwrap();

        let stream = ds.tracked_files().await.unwrap();
        let batches = collect_rows(stream).await;

        let types: HashSet<String> = collect_column_values(&batches, "type")
            .into_iter()
            .collect();
        assert!(
            types.contains("transaction file"),
            "expected 'transaction file' rows; got types: {:?}",
            types
        );
    }

    #[tokio::test]
    async fn test_tracked_files_index() {
        let uri = "memory://test_tracked_files_index";

        let mut ds = Dataset::write(some_batch(), uri, None).await.unwrap();
        let params = VectorIndexParams::ivf_pq(2, 8, 2, MetricType::L2, 5);
        ds.create_index(&["indexable"], IndexType::Vector, None, &params, true)
            .await
            .unwrap();

        let stream = ds.tracked_files().await.unwrap();
        let batches = collect_rows(stream).await;

        let types: HashSet<String> = collect_column_values(&batches, "type")
            .into_iter()
            .collect();
        assert!(
            types.contains("index file"),
            "expected 'index file' rows after vector index creation; got types: {:?}",
            types
        );
    }

    #[tokio::test]
    async fn test_all_files_basic() {
        let uri = "memory://test_all_files_basic";
        let ds = Dataset::write(make_simple_batch(), uri, None)
            .await
            .unwrap();

        let stream = ds.all_files().await;
        let schema = stream.schema();
        let batches = collect_rows(stream).await;

        assert_eq!(schema.field(0).name(), "base_uri");
        assert_eq!(schema.field(1).name(), "path");
        assert_eq!(schema.field(2).name(), "size_bytes");
        assert_eq!(schema.field(3).name(), "last_modified");

        let n = count_rows(&batches);
        // A dataset always has at least a manifest and a data file.
        assert!(n >= 2, "expected at least 2 physical files, got {n}");

        // Verify sizes and timestamps are populated (non-zero).
        for batch in &batches {
            let sizes = batch
                .column_by_name("size_bytes")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap();
            for i in 0..sizes.len() {
                assert!(
                    sizes.value(i) > 0,
                    "size_bytes should be positive, got {}",
                    sizes.value(i)
                );
            }

            let ts = batch
                .column_by_name("last_modified")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
                .unwrap();
            for i in 0..ts.len() {
                assert!(
                    ts.value(i) > 0,
                    "last_modified should be positive, got {}",
                    ts.value(i)
                );
            }
        }
    }

    #[tokio::test]
    async fn test_all_files_schema() {
        let uri = "memory://test_all_files_schema";
        let ds = Dataset::write(make_simple_batch(), uri, None)
            .await
            .unwrap();

        let stream = ds.all_files().await;
        let schema = stream.schema();

        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(0).name(), "base_uri");
        assert!(matches!(
            schema.field(0).data_type(),
            DataType::Dictionary(_, _)
        ));
        assert_eq!(schema.field(1).name(), "path");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).name(), "size_bytes");
        assert_eq!(schema.field(2).data_type(), &DataType::Int64);
        assert_eq!(schema.field(3).name(), "last_modified");
        assert!(matches!(
            schema.field(3).data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, _)
        ));
    }
}
