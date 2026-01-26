// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Tests for index lifecycle operations: create, optimize, partition split/join.

use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::Float32Type;
use arrow_array::{
    Array, FixedSizeListArray, Float32Array, RecordBatch, RecordBatchIterator, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use lance::dataset::{WriteMode, WriteParams};
use lance::Dataset;
use lance_arrow::FixedSizeListArrayExt;
use lance_core::utils::tempfile::TempStrDir;
use lance_index::optimize::OptimizeOptions;
use lance_index::vector::ivf::IvfBuildParams;
use lance_index::vector::pq::PQBuildParams;
use lance_index::{DatasetIndexExt, IndexType};
use lance_linalg::distance::DistanceType;
use lance_testing::datagen::generate_random_array;

use super::utils::{append_dataset, generate_test_dataset, DIM};

#[tokio::test]
async fn test_optimize_with_empty_partition() {
    let test_dir = TempStrDir::default();
    let test_uri = test_dir.as_str();
    let (mut dataset, _) = generate_test_dataset::<Float32Type>(test_uri, 0.0..1.0).await;

    let num_rows = dataset.count_rows(None).await.unwrap();
    // Create more partitions than rows to ensure some are empty
    let nlist = num_rows + 2;
    let centroids = generate_random_array(nlist * DIM);
    let ivf_centroids = FixedSizeListArray::try_new_from_values(centroids, DIM as i32).unwrap();
    let ivf_params = IvfBuildParams::try_with_centroids(nlist, Arc::new(ivf_centroids)).unwrap();
    let params = lance::index::vector::VectorIndexParams::with_ivf_pq_params(
        DistanceType::Cosine,
        ivf_params,
        PQBuildParams::default(),
    );
    dataset
        .create_index(&["vector"], IndexType::Vector, None, &params, true)
        .await
        .unwrap();

    append_dataset::<Float32Type>(&mut dataset, 1, 0.0..1.0).await;
    dataset
        .optimize_indices(&OptimizeOptions::new())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_remap_join_on_second_delta() {
    use lance::dataset::optimize::{compact_files, CompactionOptions};

    const INDEX_NAME: &str = "vector_idx";
    const BASE_ROWS_PER_PARTITION: usize = 3_000;
    const SMALL_APPEND_ROWS: usize = 64;
    let offsets = [-50.0, 50.0];

    let test_dir = TempStrDir::default();
    let test_uri = test_dir.as_str();

    let (batch, schema) = generate_clustered_batch(BASE_ROWS_PER_PARTITION, offsets);
    let batches = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
    let mut dataset = Dataset::write(
        batches,
        test_uri,
        Some(WriteParams {
            mode: WriteMode::Overwrite,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let centroids = build_centroids_for_offsets(&offsets);
    let ivf_params = IvfBuildParams::try_with_centroids(2, centroids).unwrap();
    let params = lance::index::vector::VectorIndexParams::with_ivf_pq_params(
        DistanceType::L2,
        ivf_params,
        PQBuildParams::default(),
    );
    dataset
        .create_index(
            &["vector"],
            IndexType::Vector,
            Some(INDEX_NAME.to_string()),
            &params,
            true,
        )
        .await
        .unwrap();

    let template_batch = dataset
        .take_rows(&[0], dataset.schema().clone())
        .await
        .unwrap();
    let template_values = template_batch["vector"]
        .as_fixed_size_list()
        .value(0)
        .as_primitive::<Float32Type>()
        .values()
        .to_vec();
    let mut append_params = WriteParams {
        max_rows_per_file: 32,
        max_rows_per_group: 32,
        ..Default::default()
    };
    append_params.mode = WriteMode::Append;
    append_constant_vector_with_params(
        &mut dataset,
        SMALL_APPEND_ROWS,
        &template_values,
        Some(append_params),
    )
    .await;

    dataset
        .optimize_indices(&OptimizeOptions::new())
        .await
        .unwrap();

    let stats_before: serde_json::Value =
        serde_json::from_str(&dataset.index_statistics(INDEX_NAME).await.unwrap()).unwrap();
    assert_eq!(stats_before["num_indices"].as_u64().unwrap(), 2);
    let partitions_before: Vec<usize> = stats_before["indices"]
        .as_array()
        .unwrap()
        .iter()
        .map(|idx| idx["num_partitions"].as_u64().unwrap() as usize)
        .collect();
    assert_eq!(partitions_before.len(), 2);
    let base_partition_count = partitions_before
        .iter()
        .copied()
        .max()
        .expect("expected at least one partition count");
    assert!(base_partition_count >= 2);
    assert!(partitions_before
        .iter()
        .all(|count| *count == base_partition_count));

    let indices_meta = dataset.load_indices_by_name(INDEX_NAME).await.unwrap();
    assert_eq!(indices_meta.len(), 2);

    compact_files(
        &mut dataset,
        CompactionOptions {
            target_rows_per_fragment: 5_000,
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();

    let mut dataset = Dataset::open(test_uri).await.unwrap();
    let stats_after_compaction: serde_json::Value =
        serde_json::from_str(&dataset.index_statistics(INDEX_NAME).await.unwrap()).unwrap();
    assert_eq!(stats_after_compaction["num_indices"].as_u64().unwrap(), 2);
    let mut partitions_after: Vec<usize> = stats_after_compaction["indices"]
        .as_array()
        .unwrap()
        .iter()
        .map(|idx| idx["num_partitions"].as_u64().unwrap() as usize)
        .collect();
    partitions_after.sort_unstable();
    assert_eq!(
        partitions_after,
        vec![base_partition_count, base_partition_count]
    );

    const LARGE_APPEND_ROWS: usize = 40_000;
    append_constant_vector(&mut dataset, LARGE_APPEND_ROWS, &template_values).await;
    dataset
        .optimize_indices(&OptimizeOptions::new())
        .await
        .unwrap();

    let dataset = Dataset::open(test_uri).await.unwrap();
    let stats_after_split: serde_json::Value =
        serde_json::from_str(&dataset.index_statistics(INDEX_NAME).await.unwrap()).unwrap();
    assert_eq!(stats_after_split["num_indices"].as_u64().unwrap(), 1);
    let final_partition_count = stats_after_split["indices"][0]["num_partitions"]
        .as_u64()
        .unwrap() as usize;
    assert_eq!(
        final_partition_count,
        base_partition_count + 1,
        "expected split to increase partitions beyond {}, got {}",
        base_partition_count,
        final_partition_count
    );
}

// NOTE: test_spfresh_join_split has been kept in the unit tests (v2.rs) because
// it requires access to private APIs like IvfPq::storage.

// Helper functions

fn generate_clustered_batch(
    rows_per_partition: usize,
    offsets: [f32; 2],
) -> (RecordBatch, Arc<Schema>) {
    use rand::{rngs::StdRng, Rng, SeedableRng};

    let num_partitions = offsets.len();
    let total_rows = rows_per_partition * num_partitions;
    let mut ids = Vec::with_capacity(total_rows);
    let mut values = Vec::with_capacity(total_rows * DIM);
    let mut rng = StdRng::seed_from_u64(42);
    for (cluster_idx, offset) in offsets.iter().enumerate() {
        for row in 0..rows_per_partition {
            ids.push((cluster_idx * rows_per_partition + row) as u64);
            for dim in 0..DIM {
                let base = if dim == 0 { *offset } else { 0.0 };
                let noise = (rng.random::<f32>() - 0.5) * 0.02;
                values.push(base + noise);
            }
        }
    }
    let ids = Arc::new(UInt64Array::from(ids));
    let vectors = Arc::new(
        FixedSizeListArray::try_new_from_values(Float32Array::from(values), DIM as i32).unwrap(),
    );
    let schema: Arc<_> = Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("vector", vectors.data_type().clone(), false),
    ])
    .into();
    let batch = RecordBatch::try_new(schema.clone(), vec![ids, vectors]).unwrap();
    (batch, schema)
}

fn build_centroids_for_offsets(offsets: &[f32]) -> Arc<FixedSizeListArray> {
    let mut centroid_values = Vec::with_capacity(offsets.len() * DIM);
    for &offset in offsets {
        for dim in 0..DIM {
            centroid_values.push(if dim == 0 { offset } else { 0.0 });
        }
    }
    Arc::new(
        FixedSizeListArray::try_new_from_values(Float32Array::from(centroid_values), DIM as i32)
            .unwrap(),
    )
}

async fn append_constant_vector(dataset: &mut Dataset, rows: usize, template: &[f32]) {
    append_constant_vector_with_params(dataset, rows, template, None).await;
}

async fn append_constant_vector_with_params(
    dataset: &mut Dataset,
    rows: usize,
    template: &[f32],
    write_params: Option<WriteParams>,
) {
    assert_eq!(
        template.len(),
        DIM,
        "Template vector should have {} dimensions",
        DIM
    );

    let start_id = dataset.count_rows(None).await.unwrap() as u64;
    let ids = Arc::new(UInt64Array::from_iter_values(
        start_id..start_id + rows as u64,
    ));
    let mut appended_values = Vec::with_capacity(rows * DIM);
    for _ in 0..rows {
        appended_values.extend_from_slice(template);
    }
    let vectors = Arc::new(
        FixedSizeListArray::try_new_from_values(Float32Array::from(appended_values), DIM as i32)
            .unwrap(),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("vector", vectors.data_type().clone(), false),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![ids, vectors]).unwrap();
    let batches = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let params = write_params.map(|mut params| {
        params.mode = WriteMode::Append;
        params
    });
    dataset.append(batches, params).await.unwrap();
}
