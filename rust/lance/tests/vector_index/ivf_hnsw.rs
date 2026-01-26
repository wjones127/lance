// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! IVF_HNSW_PQ and IVF_HNSW_PQ_4bit index tests.

use arrow::datatypes::Float32Type;
use lance::index::vector::VectorIndexParams;
use lance_core::utils::tempfile::TempStrDir;
use lance_index::vector::hnsw::builder::HnswBuildParams;
use lance_index::vector::ivf::IvfBuildParams;
use lance_index::vector::pq::PQBuildParams;
use lance_index::{DatasetIndexExt, IndexType};
use lance_linalg::distance::DistanceType;
use rstest::rstest;

use super::utils::{generate_test_dataset, test_recall, DIM};

#[rstest]
#[case::l2(4, DistanceType::L2, 0.9)]
#[case::cosine(4, DistanceType::Cosine, 0.9)]
#[case::dot(4, DistanceType::Dot, 0.85)]
#[tokio::test]
async fn test_create_ivf_hnsw_pq(
    #[case] nlist: usize,
    #[case] distance_type: DistanceType,
    #[case] recall_requirement: f32,
) {
    let test_dir = TempStrDir::default();
    let test_uri = test_dir.as_str();
    let (mut dataset, vectors) = generate_test_dataset::<Float32Type>(test_uri, 0.0..1.0).await;

    let ivf_params = IvfBuildParams::new(nlist);
    let pq_params = PQBuildParams::default();
    let hnsw_params = HnswBuildParams::default();
    let params = VectorIndexParams::with_ivf_hnsw_pq_params(
        distance_type,
        ivf_params,
        hnsw_params,
        pq_params,
    );

    let vector_column = "vector";
    dataset
        .create_index(&[vector_column], IndexType::Vector, None, &params, true)
        .await
        .unwrap();

    test_recall::<Float32Type>(
        nlist,
        recall_requirement,
        vector_column,
        &dataset,
        vectors,
        params.metric_type,
    )
    .await;
}

#[rstest]
#[case::l2(4, DistanceType::L2, 0.85)]
#[case::cosine(4, DistanceType::Cosine, 0.85)]
#[case::dot(4, DistanceType::Dot, 0.8)]
#[tokio::test]
async fn test_create_ivf_hnsw_pq_4bit(
    #[case] nlist: usize,
    #[case] distance_type: DistanceType,
    #[case] recall_requirement: f32,
) {
    let test_dir = TempStrDir::default();
    let test_uri = test_dir.as_str();
    let (mut dataset, vectors) = generate_test_dataset::<Float32Type>(test_uri, 0.0..1.0).await;

    let ivf_params = IvfBuildParams::new(nlist);
    let pq_params = PQBuildParams::new(DIM, 4);
    let hnsw_params = HnswBuildParams::default();
    let params = VectorIndexParams::with_ivf_hnsw_pq_params(
        distance_type,
        ivf_params,
        hnsw_params,
        pq_params,
    );

    let vector_column = "vector";
    dataset
        .create_index(&[vector_column], IndexType::Vector, None, &params, true)
        .await
        .unwrap();

    test_recall::<Float32Type>(
        nlist,
        recall_requirement,
        vector_column,
        &dataset,
        vectors,
        params.metric_type,
    )
    .await;
}
