// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Tests for PQ model building.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::Float32Type;
use arrow_array::{Array, FixedSizeListArray, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};
use lance::index::vector::ivf::build_ivf_model;
use lance::index::vector::pq::build_pq_model;
use lance::Dataset;
use lance_arrow::FixedSizeListArrayExt;
use lance_core::utils::tempfile::TempStrDir;
use lance_index::vector::ivf::storage::IvfModel;
use lance_index::vector::ivf::IvfBuildParams;
use lance_index::vector::pq::PQBuildParams;
use lance_index::vector::quantizer::Quantization;
use lance_linalg::distance::MetricType;
use lance_linalg::kernels::normalize_fsl;
use lance_testing::datagen::generate_random_array_with_range;

const DIM: usize = 128;

async fn generate_dataset(test_uri: &str, range: Range<f32>) -> (Dataset, Arc<FixedSizeListArray>) {
    let vectors = generate_random_array_with_range::<Float32Type>(1000 * DIM, range);
    let metadata: HashMap<String, String> = vec![("test".to_string(), "ivf_pq".to_string())]
        .into_iter()
        .collect();

    let schema = Arc::new(
        Schema::new(vec![Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIM as i32,
            ),
            true,
        )])
        .with_metadata(metadata),
    );
    let fsl = Arc::new(FixedSizeListArray::try_new_from_values(vectors, DIM as i32).unwrap());
    let batch = RecordBatch::try_new(schema.clone(), vec![fsl.clone()]).unwrap();

    let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());
    (Dataset::write(batches, test_uri, None).await.unwrap(), fsl)
}

#[tokio::test]
async fn test_build_pq_model_l2() {
    let test_dir = TempStrDir::default();
    let test_uri = test_dir.as_str();

    let (dataset, _) = generate_dataset(test_uri, 100.0..120.0).await;

    let centroids = generate_random_array_with_range::<Float32Type>(4 * DIM, -1.0..1.0);
    let fsl = FixedSizeListArray::try_new_from_values(centroids, DIM as i32).unwrap();
    let ivf = IvfModel::new(fsl, None);
    let params = PQBuildParams::new(16, 8);
    let pq = build_pq_model(&dataset, "vector", DIM, MetricType::L2, &params, Some(&ivf))
        .await
        .unwrap();

    assert_eq!(pq.code_dim(), 16);
    assert_eq!(pq.num_bits, 8);
    assert_eq!(pq.dimension, DIM);

    let codebook = pq.codebook;
    assert_eq!(codebook.len(), 256);
    codebook
        .values()
        .as_primitive::<Float32Type>()
        .values()
        .iter()
        .for_each(|v| {
            assert!((99.0..121.0).contains(v));
        });
}

#[tokio::test]
async fn test_build_pq_model_cosine() {
    use arrow::array::AsArray;

    let test_dir = TempStrDir::default();
    let test_uri = test_dir.as_str();

    let (dataset, vectors) = generate_dataset(test_uri, 100.0..120.0).await;

    let ivf_params = IvfBuildParams::new(4);
    let ivf = build_ivf_model(&dataset, "vector", DIM, MetricType::Cosine, &ivf_params)
        .await
        .unwrap();
    let params = PQBuildParams::new(16, 8);
    let pq = build_pq_model(
        &dataset,
        "vector",
        DIM,
        MetricType::Cosine,
        &params,
        Some(&ivf),
    )
    .await
    .unwrap();

    assert_eq!(pq.code_dim(), 16);
    assert_eq!(pq.num_bits, 8);
    assert_eq!(pq.dimension, DIM);

    #[allow(clippy::redundant_clone)]
    let codebook = pq.codebook.clone();
    assert_eq!(codebook.len(), 256);
    codebook
        .values()
        .as_primitive::<Float32Type>()
        .values()
        .iter()
        .for_each(|v| {
            assert!((-1.0..1.0).contains(v));
        });

    let vectors = normalize_fsl(&vectors).unwrap();
    let row = vectors.slice(0, 1);

    let ivf2 = lance_index::vector::ivf::new_ivf_transformer(
        ivf.centroids.clone().unwrap(),
        MetricType::L2,
        vec![],
    );

    let residual_query = ivf2.compute_residual(&row).unwrap();
    let pq_code = pq.quantize(&residual_query).unwrap();
    let distances = pq
        .compute_distances(
            &residual_query.value(0),
            pq_code.as_fixed_size_list().values().as_primitive(),
        )
        .unwrap();
    assert!(
        distances.values().iter().all(|&d| d <= 0.001),
        "distances: {:?}",
        distances
    );
}
