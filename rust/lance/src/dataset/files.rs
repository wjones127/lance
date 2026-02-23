// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Dataset file inspection APIs.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, LazyLock};

use arrow_array::builder::{
    Int64Builder, StringBuilder, StringDictionaryBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::types::Int32Type;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{stream, StreamExt, TryStreamExt};
use object_store::path::Path;

use crate::dataset::{DATA_DIR, INDICES_DIR, TRANSACTIONS_DIR};
use crate::Dataset;
use lance_core::Result;
use lance_table::io::commit::ManifestLocation;
use lance_table::io::deletion::relative_deletion_file_path;
use lance_table::io::manifest::{read_manifest, read_manifest_indexes};

const BATCH_SIZE: usize = 4096;

static TRACKED_FILES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("version", DataType::Int64, false),
        Field::new(
            "base_uri",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("path", DataType::Utf8, false),
        Field::new(
            "type",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
    ]))
});

static ALL_FILES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new(
            "base_uri",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("path", DataType::Utf8, false),
        Field::new("size_bytes", DataType::Int64, false),
        Field::new(
            "last_modified",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ]))
});

fn remove_prefix(path: &Path, prefix: &Path) -> Path {
    match path.prefix_match(prefix) {
        Some(parts) => Path::from_iter(parts),
        None => path.clone(),
    }
}

/// A single row destined for the `tracked_files` output.
struct FileRow {
    version: u64,
    base_uri: String,
    path: String,
    file_type: &'static str,
}

/// Arrow batch builder for the `tracked_files` schema.
///
/// Construct with [`with_capacity`](Self::with_capacity) to pre-size the
/// underlying buffers, then call [`extend`](Self::extend) to fill rows in bulk.
struct TrackedFileBatch {
    schema: SchemaRef,
    version: Int64Builder,
    base_uri: StringDictionaryBuilder<Int32Type>,
    path: StringBuilder,
    file_type: StringDictionaryBuilder<Int32Type>,
}

impl TrackedFileBatch {
    fn with_capacity(schema: SchemaRef, capacity: usize) -> Self {
        // Guess 4 unique base_uris and 20 bytes per uri/type value.
        Self {
            schema,
            version: Int64Builder::with_capacity(capacity),
            base_uri: StringDictionaryBuilder::with_capacity(capacity, 4, capacity * 20),
            path: StringBuilder::with_capacity(capacity, capacity * 50),
            file_type: StringDictionaryBuilder::with_capacity(capacity, 5, capacity * 20),
        }
    }

    fn extend<'a>(&mut self, rows: impl IntoIterator<Item = &'a FileRow>) {
        for row in rows {
            self.version.append_value(row.version as i64);
            self.base_uri.append_value(&row.base_uri);
            self.path.append_value(&row.path);
            self.file_type.append_value(row.file_type);
        }
    }

    fn finish(mut self) -> Result<RecordBatch> {
        RecordBatch::try_new(
            self.schema,
            vec![
                Arc::new(self.version.finish()),
                Arc::new(self.base_uri.finish()),
                Arc::new(self.path.finish()),
                Arc::new(self.file_type.finish()),
            ],
        )
        .map_err(Into::into)
    }
}

/// Build one `RecordBatch` per `BATCH_SIZE` chunk of `rows`.
fn rows_to_batches<'a>(
    rows: &'a [FileRow],
    schema: &'a SchemaRef,
) -> impl Iterator<Item = Result<RecordBatch>> + 'a {
    rows.chunks(BATCH_SIZE).map(|chunk| {
        let mut b = TrackedFileBatch::with_capacity(schema.clone(), chunk.len());
        b.extend(chunk);
        b.finish()
    })
}

/// Collect every non-index [`FileRow`] referenced by a single manifest.
fn manifest_file_rows(
    version: u64,
    manifest_path_rel: &str,
    manifest: &lance_table::format::Manifest,
    uri: &str,
) -> Vec<FileRow> {
    let n_data: usize = manifest.fragments.iter().map(|f| f.files.len()).sum();
    let n_del = manifest
        .fragments
        .iter()
        .filter(|f| f.deletion_file.is_some())
        .count();
    let n_tx = manifest.transaction_file.is_some() as usize;
    let mut rows = Vec::with_capacity(1 + n_data + n_del + n_tx);

    rows.push(FileRow {
        version,
        base_uri: uri.to_string(),
        path: manifest_path_rel.to_string(),
        file_type: "manifest",
    });

    for fragment in manifest.fragments.iter() {
        for data_file in fragment.files.iter() {
            let file_base_uri = data_file
                .base_id
                .and_then(|id| manifest.base_paths.get(&id))
                .map(|bp| bp.path.as_str())
                .unwrap_or(uri)
                .to_string();
            rows.push(FileRow {
                version,
                base_uri: file_base_uri,
                path: format!("{}/{}", DATA_DIR, data_file.path),
                file_type: "data file",
            });
        }
        if let Some(del_file) = &fragment.deletion_file {
            let del_base_uri = del_file
                .base_id
                .and_then(|id| manifest.base_paths.get(&id))
                .map(|bp| bp.path.as_str())
                .unwrap_or(uri)
                .to_string();
            rows.push(FileRow {
                version,
                base_uri: del_base_uri,
                path: relative_deletion_file_path(fragment.id, del_file),
                file_type: "deletion file",
            });
        }
    }

    if let Some(tx_file) = &manifest.transaction_file {
        rows.push(FileRow {
            version,
            base_uri: uri.to_string(),
            path: format!("{}/{}", TRANSACTIONS_DIR, tx_file),
            file_type: "transaction file",
        });
    }

    rows
}

/// State for the Phase 2 stream in [`Dataset::tracked_files`].
///
/// Phase 2 lists each index UUID directory and emits one row per
/// (version, index-file) pair.
struct IndexFilesState {
    /// Receives the UUID-to-versions map from the Phase 1 spawned task.
    /// `None` once the map has been received.
    rx_uuids: Option<tokio::sync::oneshot::Receiver<HashMap<String, Vec<u64>>>>,
    /// Iterates over the UUID map entries after `rx_uuids` has been consumed.
    uuid_iter: Option<std::collections::hash_map::IntoIter<String, Vec<u64>>>,
    /// Batches that have been built but not yet yielded.
    pending: VecDeque<RecordBatch>,
    object_store: lance_io::object_store::ObjectStore,
    base: Path,
    uri: String,
    schema: SchemaRef,
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
        let schema = TRACKED_FILES_SCHEMA.clone();
        let io_parallelism = self.object_store.io_parallelism();
        let base = self.base.clone();
        let uri = self.uri().to_string();
        let object_store = self.object_store.clone();

        // Pre-collect manifest locations (path + optional size only — cheap).
        // This lets the spawned task be 'static without borrowing &self.
        let manifest_locations: Vec<ManifestLocation> = self
            .commit_handler
            .list_manifest_locations(&self.base, &self.object_store, false)
            .try_collect()
            .await?;

        // Phase 1: spawn a task that processes manifests concurrently, sends
        // non-index batches via an mpsc channel, and delivers the UUID→versions
        // map via a oneshot when done.  Phase 2 is driven by the caller-side
        // `try_unfold` once it has received the UUID map from the oneshot.
        let (tx_batches, rx_batches) =
            tokio::sync::mpsc::channel::<datafusion::error::Result<RecordBatch>>(16);
        let (tx_uuids, rx_uuids) = tokio::sync::oneshot::channel::<HashMap<String, Vec<u64>>>();

        let base_p1 = base.clone();
        let uri_p1 = uri.clone();
        let os_p1 = object_store.clone();
        let schema_p1 = schema.clone();

        tokio::spawn(async move {
            let tx_per_manifest = tx_batches.clone();

            let uuid_map_result: lance_core::Result<HashMap<String, Vec<u64>>> = stream::iter(
                manifest_locations
                    .into_iter()
                    .map(Ok::<_, lance_core::Error>),
            )
            .map(move |loc_res| {
                let base = base_p1.clone();
                let uri = uri_p1.clone();
                let os = os_p1.clone();
                let schema = schema_p1.clone();
                let tx = tx_per_manifest.clone();
                async move {
                    let loc = loc_res?;
                    let manifest = read_manifest(&os, &loc.path, loc.size).await?;
                    let indexes = read_manifest_indexes(&os, &loc, &manifest).await?;
                    let version = manifest.version;

                    let manifest_rel = remove_prefix(&loc.path, &base);
                    let rows = manifest_file_rows(version, manifest_rel.as_ref(), &manifest, &uri);

                    for batch_result in rows_to_batches(&rows, &schema) {
                        let df = batch_result.map_err(datafusion::error::DataFusionError::from);
                        if tx.send(df).await.is_err() {
                            return Ok::<_, lance_core::Error>(vec![]);
                        }
                    }

                    let pairs: Vec<(u64, String)> = indexes
                        .iter()
                        .map(|i| (version, i.uuid.to_string()))
                        .collect();
                    Ok(pairs)
                }
            })
            .buffer_unordered(io_parallelism)
            .try_fold(
                HashMap::<String, Vec<u64>>::new(),
                |mut map, pairs| async move {
                    for (v, u) in pairs {
                        map.entry(u).or_default().push(v);
                    }
                    Ok(map)
                },
            )
            .await;

            // tx_batches dropped here (all per-manifest clones + the clone above).
            // This closes the mpsc channel, signalling end-of-phase-1.
            match uuid_map_result {
                Ok(map) => {
                    tx_uuids.send(map).ok();
                }
                Err(e) => {
                    // Send the error through the batch channel so the phase 1
                    // stream propagates it to the caller.  The oneshot is just
                    // dropped, so phase 2 will get a RecvError — but it will
                    // never be polled because the chain stops at the first error.
                    tx_batches
                        .send(Err(datafusion::error::DataFusionError::from(e)))
                        .await
                        .ok();
                }
            }
        });

        // Phase 1 stream: yield batches produced by the spawned task.
        let phase1_stream = stream::try_unfold(rx_batches, |mut rx| async move {
            match rx.recv().await {
                Some(Ok(batch)) => Ok(Some((batch, rx))),
                Some(Err(e)) => Err(e),
                None => Ok(None),
            }
        });

        // Phase 2 stream: once the UUID map arrives, list each index directory
        // and emit (version, index-file) rows.  The `try_unfold` state machine
        // processes one UUID directory per invocation.
        let phase2_stream = stream::try_unfold(
            IndexFilesState {
                rx_uuids: Some(rx_uuids),
                uuid_iter: None,
                pending: VecDeque::new(),
                object_store: (*object_store).clone(),
                base,
                uri,
                schema: schema.clone(),
            },
            |mut state| async move {
                // Step 1: receive the UUID map on the first call.
                if let Some(rx) = state.rx_uuids.take() {
                    // If Phase 1 failed, rx will return Err(RecvError).  In
                    // that case the phase 1 stream already propagated the error
                    // and this stream will never be polled — but we still handle
                    // the case gracefully.
                    match rx.await {
                        Ok(map) => state.uuid_iter = Some(map.into_iter()),
                        Err(_) => return Ok::<_, lance_core::Error>(None),
                    }
                }

                // Step 2: emit any batches already queued from a previous call.
                if let Some(batch) = state.pending.pop_front() {
                    return Ok(Some((batch, state)));
                }

                // Step 3: process the next UUID directory.  Loop until we either
                // produce a batch or exhaust all UUIDs.
                let iter = state.uuid_iter.as_mut().expect("initialized in step 1");
                loop {
                    let Some((uuid, versions)) = iter.next() else {
                        return Ok(None);
                    };

                    let index_dir = state.base.child(INDICES_DIR).child(uuid.as_str());
                    // Collect files eagerly so we don't hold a live stream that
                    // borrows object_store across the try_unfold state boundary.
                    let files: Vec<object_store::ObjectMeta> = state
                        .object_store
                        .read_dir_all(&index_dir, None)
                        .try_collect()
                        .await?;

                    for meta in &files {
                        let rel = remove_prefix(&meta.location, &state.base);
                        let rows: Vec<FileRow> = versions
                            .iter()
                            .map(|&v| FileRow {
                                version: v,
                                base_uri: state.uri.clone(),
                                path: rel.as_ref().to_string(),
                                file_type: "index file",
                            })
                            .collect();
                        for batch_result in rows_to_batches(&rows, &state.schema) {
                            state.pending.push_back(batch_result?);
                        }
                    }

                    if let Some(batch) = state.pending.pop_front() {
                        return Ok(Some((batch, state)));
                    }
                    // No files for this UUID; try the next one.
                }
            },
        )
        // Convert lance_core::Error to DataFusionError for the stream adapter.
        .map_err(datafusion::error::DataFusionError::from);

        let combined = phase1_stream.chain(phase2_stream);

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, combined)))
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
    pub async fn all_files(&self) -> Result<SendableRecordBatchStream> {
        let schema = ALL_FILES_SCHEMA.clone();
        let base = self.base.clone();
        let uri = self.uri().to_string();
        let object_store = self.object_store.clone();

        // Spawn a task that drives read_dir_all (which borrows object_store)
        // and sends batches via an mpsc channel.  This avoids returning a stream
        // that captures &self.
        let (tx, rx) = tokio::sync::mpsc::channel::<datafusion::error::Result<RecordBatch>>(16);

        let schema_spawn = schema.clone();
        tokio::spawn(async move {
            // object_store is an owned Arc<ObjectStore>. read_dir_all borrows it
            // within this async block; Rust's async state machine handles the
            // resulting self-referential type safely via Pin.
            let mut file_stream = object_store.read_dir_all(&base, None);
            let mut chunk: Vec<object_store::ObjectMeta> = Vec::with_capacity(BATCH_SIZE);

            loop {
                match file_stream.next().await {
                    Some(Ok(meta)) => {
                        chunk.push(meta);
                        if chunk.len() >= BATCH_SIZE {
                            let batch = build_all_files_batch(&chunk, &schema_spawn, &base, &uri);
                            if tx
                                .send(batch.map_err(datafusion::error::DataFusionError::from))
                                .await
                                .is_err()
                            {
                                return;
                            }
                            chunk.clear();
                        }
                    }
                    Some(Err(e)) => {
                        tx.send(Err(datafusion::error::DataFusionError::from(e)))
                            .await
                            .ok();
                        return;
                    }
                    None => break,
                }
            }

            if !chunk.is_empty() {
                let batch = build_all_files_batch(&chunk, &schema_spawn, &base, &uri);
                tx.send(batch.map_err(datafusion::error::DataFusionError::from))
                    .await
                    .ok();
            }
        });

        let batch_stream = stream::try_unfold(rx, |mut rx| async move {
            match rx.recv().await {
                Some(Ok(batch)) => Ok(Some((batch, rx))),
                Some(Err(e)) => Err(e),
                None => Ok(None),
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            batch_stream,
        )))
    }
}

fn build_all_files_batch(
    chunk: &[object_store::ObjectMeta],
    schema: &SchemaRef,
    base: &Path,
    uri: &str,
) -> Result<RecordBatch> {
    let n = chunk.len();
    let mut base_uri_builder =
        StringDictionaryBuilder::<Int32Type>::with_capacity(n, 1, uri.len() * n);
    let mut path_builder = StringBuilder::with_capacity(n, n * 50);
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
        schema.clone(),
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
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
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

    fn collect_column_values(batches: &[RecordBatch], col: &str) -> Vec<String> {
        batches
            .iter()
            .flat_map(|b| {
                let col = b.column_by_name(col).unwrap();
                (0..col.len()).map(|i| {
                    let dict = col
                        .as_any()
                        .downcast_ref::<arrow_array::DictionaryArray<
                            arrow_array::types::Int32Type,
                        >>()
                        .unwrap();
                    let values = dict
                        .values()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    let key = dict.keys().value(i) as usize;
                    values.value(key).to_string()
                })
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

        let stream = ds.all_files().await.unwrap();
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

        let stream = ds.all_files().await.unwrap();
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
