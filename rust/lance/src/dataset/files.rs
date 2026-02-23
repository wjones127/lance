// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Dataset file inspection APIs.

use std::collections::HashMap;
use std::sync::Arc;

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
use lance_table::io::deletion::relative_deletion_file_path;
use lance_table::io::manifest::{read_manifest, read_manifest_indexes};

const BATCH_SIZE: usize = 4096;

fn tracked_files_schema() -> SchemaRef {
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
}

fn all_files_schema() -> SchemaRef {
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
}

fn remove_prefix(path: &Path, prefix: &Path) -> Path {
    match path.prefix_match(prefix) {
        Some(parts) => Path::from_iter(parts),
        None => path.clone(),
    }
}

struct TrackedFileBatch {
    schema: SchemaRef,
    version: Int64Builder,
    base_uri: StringDictionaryBuilder<Int32Type>,
    path: StringBuilder,
    file_type: StringDictionaryBuilder<Int32Type>,
    count: usize,
}

impl TrackedFileBatch {
    fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            version: Int64Builder::new(),
            base_uri: StringDictionaryBuilder::new(),
            path: StringBuilder::new(),
            file_type: StringDictionaryBuilder::new(),
            count: 0,
        }
    }

    fn append(&mut self, version: u64, base_uri: &str, path: &str, file_type: &str) {
        self.version.append_value(version as i64);
        self.base_uri.append_value(base_uri);
        self.path.append_value(path);
        self.file_type.append_value(file_type);
        self.count += 1;
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(self.version.finish()),
                Arc::new(self.base_uri.finish()),
                Arc::new(self.path.finish()),
                Arc::new(self.file_type.finish()),
            ],
        )?;
        self.count = 0;
        Ok(batch)
    }
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
        let schema = tracked_files_schema();
        let io_parallelism = self.object_store.io_parallelism();
        let base = self.base.clone();
        let uri = self.uri().to_string();
        let object_store = self.object_store.clone();

        // Process all manifests concurrently, collecting non-index rows and (version, uuid) pairs.
        // Index UUID pairs are small (just strings) so we collect them all before Phase 2.
        let manifest_stream =
            self.commit_handler
                .list_manifest_locations(&self.base, &self.object_store, false);

        let base_c = base.clone();
        let uri_c = uri.clone();
        let os_c = object_store.clone();
        let schema_c = schema.clone();

        #[allow(clippy::type_complexity)]
        let processed: Vec<(Vec<RecordBatch>, Vec<(u64, String)>)> = manifest_stream
            .map(move |loc_res| {
                let base = base_c.clone();
                let uri = uri_c.clone();
                let os = os_c.clone();
                let schema = schema_c.clone();
                async move {
                    let loc = loc_res?;
                    let manifest = read_manifest(&os, &loc.path, loc.size).await?;
                    let indexes = read_manifest_indexes(&os, &loc, &manifest).await?;
                    let version = manifest.version;

                    let mut builder = TrackedFileBatch::new(schema);
                    let mut batches: Vec<RecordBatch> = Vec::new();
                    let mut index_pairs: Vec<(u64, String)> = Vec::new();

                    // Manifest file itself.
                    let manifest_rel = remove_prefix(&loc.path, &base);
                    builder.append(version, &uri, manifest_rel.as_ref(), "manifest");
                    if builder.count >= BATCH_SIZE {
                        batches.push(builder.finish()?);
                    }

                    // Per-fragment data files and deletion files.
                    for fragment in manifest.fragments.iter() {
                        for data_file in fragment.files.iter() {
                            let file_base_uri = data_file
                                .base_id
                                .and_then(|id| manifest.base_paths.get(&id))
                                .map(|bp| bp.path.as_str())
                                .unwrap_or(uri.as_str())
                                .to_string();
                            let path = format!("{}/{}", DATA_DIR, data_file.path);
                            builder.append(version, &file_base_uri, &path, "data file");
                            if builder.count >= BATCH_SIZE {
                                batches.push(builder.finish()?);
                            }
                        }
                        if let Some(del_file) = &fragment.deletion_file {
                            let del_base_uri = del_file
                                .base_id
                                .and_then(|id| manifest.base_paths.get(&id))
                                .map(|bp| bp.path.as_str())
                                .unwrap_or(uri.as_str())
                                .to_string();
                            let path = relative_deletion_file_path(fragment.id, del_file);
                            builder.append(version, &del_base_uri, &path, "deletion file");
                            if builder.count >= BATCH_SIZE {
                                batches.push(builder.finish()?);
                            }
                        }
                    }

                    // Transaction file.
                    if let Some(tx_file) = &manifest.transaction_file {
                        let path = format!("{}/{}", TRANSACTIONS_DIR, tx_file);
                        builder.append(version, &uri, &path, "transaction file");
                        if builder.count >= BATCH_SIZE {
                            batches.push(builder.finish()?);
                        }
                    }

                    for index in &indexes {
                        index_pairs.push((version, index.uuid.to_string()));
                    }

                    if !builder.is_empty() {
                        batches.push(builder.finish()?);
                    }

                    Ok::<_, lance_core::Error>((batches, index_pairs))
                }
            })
            .buffer_unordered(io_parallelism)
            .try_collect()
            .await?;

        let mut phase1_batches: Vec<RecordBatch> = Vec::new();
        // uuid -> all versions that reference it
        let mut uuid_to_versions: HashMap<String, Vec<u64>> = HashMap::new();
        for (batches, pairs) in processed {
            phase1_batches.extend(batches);
            for (version, uuid) in pairs {
                uuid_to_versions.entry(uuid).or_default().push(version);
            }
        }

        // Phase 2: list files within each unique index UUID directory and emit one row per
        // (version, file) pair so callers can see which versions reference each index file.
        let mut phase2_batches: Vec<RecordBatch> = Vec::new();
        let mut builder = TrackedFileBatch::new(schema.clone());
        for (uuid, versions) in &uuid_to_versions {
            let index_dir = base.child(INDICES_DIR).child(uuid.as_str());
            let mut file_stream = object_store.read_dir_all(&index_dir, None);
            while let Some(meta_res) = file_stream.next().await {
                let meta = meta_res?;
                let rel_path = remove_prefix(&meta.location, &base);
                for version in versions {
                    builder.append(*version, &uri, rel_path.as_ref(), "index file");
                    if builder.count >= BATCH_SIZE {
                        phase2_batches.push(builder.finish()?);
                    }
                }
            }
        }
        if !builder.is_empty() {
            phase2_batches.push(builder.finish()?);
        }

        let all_batches: Vec<datafusion::error::Result<RecordBatch>> = phase1_batches
            .into_iter()
            .chain(phase2_batches)
            .map(Ok)
            .collect();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter(all_batches),
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
    pub async fn all_files(&self) -> Result<SendableRecordBatchStream> {
        let schema = all_files_schema();
        let base = self.base.clone();
        let uri = self.uri().to_string();

        let mut file_stream = self.object_store.read_dir_all(&base, None);

        let mut base_uri_builder: StringDictionaryBuilder<Int32Type> =
            StringDictionaryBuilder::new();
        let mut path_builder = StringBuilder::new();
        let mut size_builder = Int64Builder::new();
        let mut ts_builder = TimestampMicrosecondBuilder::new().with_timezone("UTC");
        let mut count = 0usize;
        let mut batches: Vec<RecordBatch> = Vec::new();

        while let Some(meta_res) = file_stream.next().await {
            let meta = meta_res?;
            let rel = remove_prefix(&meta.location, &base);
            base_uri_builder.append_value(&uri);
            path_builder.append_value(rel.as_ref());
            size_builder.append_value(meta.size as i64);
            ts_builder.append_value(meta.last_modified.timestamp_micros());
            count += 1;
            if count >= BATCH_SIZE {
                batches.push(RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(base_uri_builder.finish()),
                        Arc::new(path_builder.finish()),
                        Arc::new(size_builder.finish()),
                        Arc::new(ts_builder.finish()),
                    ],
                )?);
                count = 0;
            }
        }

        if count > 0 {
            batches.push(RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(base_uri_builder.finish()),
                    Arc::new(path_builder.finish()),
                    Arc::new(size_builder.finish()),
                    Arc::new(ts_builder.finish()),
                ],
            )?);
        }

        let df_batches: Vec<datafusion::error::Result<RecordBatch>> =
            batches.into_iter().map(Ok).collect();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter(df_batches),
        )))
    }
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
                // Dictionary-encoded column: cast to string view
                let vals: Vec<String> = (0..col.len())
                    .map(|i| {
                        // dict col -> string col
                        let dict = col.as_any().downcast_ref::<arrow_array::DictionaryArray<arrow_array::types::Int32Type>>().unwrap();
                        let values = dict.values().as_any().downcast_ref::<StringArray>().unwrap();
                        let key = dict.keys().value(i) as usize;
                        values.value(key).to_string()
                    })
                    .collect();
                vals
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
