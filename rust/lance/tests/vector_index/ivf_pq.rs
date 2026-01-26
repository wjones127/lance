// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! IVF_PQ and IVF_PQ_V3 index tests.

use std::sync::Arc;

use arrow::datatypes::Float32Type;
use lance::index::vector::{IndexFileVersion, VectorIndexParams};
use lance_core::utils::tempfile::TempStrDir;
use lance_index::vector::ivf::IvfBuildParams;
use lance_index::vector::pq::PQBuildParams;
use lance_index::{DatasetIndexExt, IndexType};
use lance_linalg::distance::DistanceType;
use rstest::rstest;

use super::utils::{generate_test_dataset, test_recall};

/// Test helper to run index creation and recall tests for various data types.
async fn test_ivf_pq_index<T: arrow_array::ArrowPrimitiveType>(
    params: VectorIndexParams,
    nlist: usize,
    recall_requirement: f32,
    range: std::ops::Range<T::Native>,
) where
    T::Native: rand::distr::uniform::SampleUniform,
{
    let test_dir = TempStrDir::default();
    let test_uri = test_dir.as_str();
    let (mut dataset, vectors) = generate_test_dataset::<T>(test_uri, range).await;

    let vector_column = "vector";
    dataset
        .create_index(&[vector_column], IndexType::Vector, None, &params, true)
        .await
        .unwrap();

    test_recall::<T>(
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
#[case::l2(4, DistanceType::L2, 0.9)]
#[case::cosine(4, DistanceType::Cosine, 0.9)]
#[case::dot(4, DistanceType::Dot, 0.85)]
#[tokio::test]
async fn test_build_ivf_pq(
    #[case] nlist: usize,
    #[case] distance_type: DistanceType,
    #[case] recall_requirement: f32,
) {
    let ivf_params = IvfBuildParams::new(nlist);
    let pq_params = PQBuildParams::default();
    let params = VectorIndexParams::with_ivf_pq_params(distance_type, ivf_params, pq_params)
        .version(IndexFileVersion::Legacy)
        .clone();

    test_ivf_pq_index::<Float32Type>(params, nlist, recall_requirement, 0.0..1.0).await;
}

#[rstest]
#[case::nlist1_l2(1, DistanceType::L2, 0.9)]
#[case::nlist1_cosine(1, DistanceType::Cosine, 0.9)]
#[case::nlist1_dot(1, DistanceType::Dot, 0.85)]
#[case::nlist4_l2(4, DistanceType::L2, 0.9)]
#[case::nlist4_cosine(4, DistanceType::Cosine, 0.9)]
#[case::nlist4_dot(4, DistanceType::Dot, 0.85)]
#[tokio::test]
async fn test_build_ivf_pq_v3(
    #[case] nlist: usize,
    #[case] distance_type: DistanceType,
    #[case] recall_requirement: f32,
) {
    let ivf_params = IvfBuildParams::new(nlist);
    let pq_params = PQBuildParams::default();
    let params = VectorIndexParams::with_ivf_pq_params(distance_type, ivf_params, pq_params);

    test_ivf_pq_index::<Float32Type>(params, nlist, recall_requirement, 0.0..1.0).await;
}

#[rstest]
#[case::l2(4, DistanceType::L2, 0.85)]
#[case::cosine(4, DistanceType::Cosine, 0.85)]
#[case::dot(4, DistanceType::Dot, 0.75)]
#[tokio::test]
async fn test_build_ivf_pq_4bit(
    #[case] nlist: usize,
    #[case] distance_type: DistanceType,
    #[case] recall_requirement: f32,
) {
    use super::utils::DIM;

    let ivf_params = IvfBuildParams::new(nlist);
    let pq_params = PQBuildParams::new(DIM, 4);
    let params = VectorIndexParams::with_ivf_pq_params(distance_type, ivf_params, pq_params);

    test_ivf_pq_index::<Float32Type>(params, nlist, recall_requirement, 0.0..1.0).await;
}

#[tokio::test]
async fn test_create_index_with_many_invalid_vectors() {
    use arrow_array::{FixedSizeListArray, Float32Array, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};
    use lance::dataset::{WriteMode, WriteParams};
    use lance::Dataset;
    use lance_arrow::FixedSizeListArrayExt;

    use super::utils::DIM;

    let test_dir = TempStrDir::default();
    let test_uri = test_dir.as_str();

    // We use 8192 batch size by default, so generate 8192 * 3 vectors to get 3 batches.
    // Generate 3 batches: first batch's vectors are all NaN, second batch is random,
    // third batch has very large values.
    let num_rows = 8192 * 3;
    let mut vectors = Vec::new();
    for i in 0..num_rows {
        if i < 8192 {
            vectors.extend(std::iter::repeat_n(f32::NAN, DIM));
        } else if i < 8192 * 2 {
            vectors.extend(std::iter::repeat_n(rand::random::<f32>(), DIM));
        } else {
            vectors.extend(std::iter::repeat_n(rand::random::<f32>() * 1e20, DIM));
        }
    }
    let schema = Schema::new(vec![Field::new(
        "vector",
        DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, true)),
            DIM as i32,
        ),
        true,
    )]);
    let schema = Arc::new(schema);
    let batch = arrow_array::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(
            FixedSizeListArray::try_new_from_values(Float32Array::from(vectors), DIM as i32)
                .unwrap(),
        )],
    )
    .unwrap();
    let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    let params = WriteParams {
        mode: WriteMode::Overwrite,
        ..Default::default()
    };
    let mut dataset = Dataset::write(batches, test_uri, Some(params))
        .await
        .unwrap();

    let params = VectorIndexParams::ivf_pq(4, 8, DIM / 8, DistanceType::Dot, 50);

    dataset
        .create_index(&["vector"], IndexType::Vector, None, &params, true)
        .await
        .unwrap();
}
