// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Tests for vector index handling of null values and empty datasets.

use arrow_array::types::{Float32Type, Int32Type};
use arrow_array::{make_array, Array, Float32Array, RecordBatch};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use lance::dataset::{Dataset, InsertBuilder};
use lance::index::vector::{initialize_vector_index, IndexFileVersion, VectorIndexParams};
use lance::index::DatasetIndexInternalExt;
use lance_core::utils::tempfile::TempStrDir;
use lance_datagen::{array, gen_batch, BatchCount, Dimension, RowCount};
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::vector::hnsw::builder::HnswBuildParams;
use lance_index::vector::ivf::IvfBuildParams;
use lance_index::vector::pq::PQBuildParams;
use lance_index::vector::sq::builder::SQBuildParams;
use lance_index::{DatasetIndexExt, IndexType};
use lance_linalg::distance::MetricType;
use rstest::rstest;

struct TestPqParams {
    num_sub_vectors: usize,
    num_bits: usize,
}

impl TestPqParams {
    fn small() -> Self {
        Self {
            num_sub_vectors: 2,
            num_bits: 8,
        }
    }
}

// Clippy doesn't like that all start with Ivf but we might have some in the future
// that _don't_ start with Ivf so I feel it is meaningful to keep the prefix
#[allow(clippy::enum_variant_names)]
enum TestIndexType {
    IvfPq { pq: TestPqParams },
    IvfHnswPq { pq: TestPqParams, num_edges: usize },
    IvfHnswSq { num_edges: usize },
    IvfFlat,
}

struct CreateIndexCase {
    metric_type: MetricType,
    num_partitions: usize,
    dimension: usize,
    index_type: TestIndexType,
}

// We test L2 and Dot, because L2 PQ uses residuals while Dot doesn't,
// so they have slightly different code paths.
#[tokio::test]
#[rstest]
#[case::ivf_pq_l2(CreateIndexCase {
    metric_type: MetricType::L2,
    num_partitions: 2,
    dimension: 16,
    index_type: TestIndexType::IvfPq { pq: TestPqParams::small() },
})]
#[case::ivf_pq_dot(CreateIndexCase {
    metric_type: MetricType::Dot,
    num_partitions: 2,
    dimension: 2000,
    index_type: TestIndexType::IvfPq { pq: TestPqParams::small() },
})]
#[case::ivf_flat(CreateIndexCase { num_partitions: 1, metric_type: MetricType::Dot, dimension: 16, index_type: TestIndexType::IvfFlat })]
#[case::ivf_hnsw_pq(CreateIndexCase {
    num_partitions: 2,
    metric_type: MetricType::Dot,
    dimension: 16,
    index_type: TestIndexType::IvfHnswPq { pq: TestPqParams::small(), num_edges: 100 },
})]
#[case::ivf_hnsw_sq(CreateIndexCase {
    metric_type: MetricType::Dot,
    num_partitions: 2,
    dimension: 16,
    index_type: TestIndexType::IvfHnswSq { num_edges: 100 },
})]
async fn test_create_index_nulls(
    #[case] test_case: CreateIndexCase,
    #[values(IndexFileVersion::Legacy, IndexFileVersion::V3)] index_version: IndexFileVersion,
) {
    let mut index_params = match test_case.index_type {
        TestIndexType::IvfPq { pq } => VectorIndexParams::with_ivf_pq_params(
            test_case.metric_type,
            IvfBuildParams::new(test_case.num_partitions),
            PQBuildParams::new(pq.num_sub_vectors, pq.num_bits),
        ),
        TestIndexType::IvfHnswPq { pq, num_edges } => VectorIndexParams::with_ivf_hnsw_pq_params(
            test_case.metric_type,
            IvfBuildParams::new(test_case.num_partitions),
            HnswBuildParams::default().num_edges(num_edges),
            PQBuildParams::new(pq.num_sub_vectors, pq.num_bits),
        ),
        TestIndexType::IvfFlat => {
            VectorIndexParams::ivf_flat(test_case.num_partitions, test_case.metric_type)
        }
        TestIndexType::IvfHnswSq { num_edges } => VectorIndexParams::with_ivf_hnsw_sq_params(
            test_case.metric_type,
            IvfBuildParams::new(test_case.num_partitions),
            HnswBuildParams::default().num_edges(num_edges),
            SQBuildParams::default(),
        ),
    };
    index_params.version(index_version);

    let nrows = 2_000;
    let data = gen_batch()
        .col(
            "vec",
            array::rand_vec::<Float32Type>(Dimension::from(test_case.dimension as u32)),
        )
        .into_batch_rows(RowCount::from(nrows))
        .unwrap();

    // Make every other row null
    let null_buffer = (0..nrows).map(|i| i % 2 == 0).collect::<BooleanBuffer>();
    let null_buffer = NullBuffer::new(null_buffer);
    let vectors = data["vec"]
        .clone()
        .to_data()
        .into_builder()
        .nulls(Some(null_buffer))
        .build()
        .unwrap();
    let vectors = make_array(vectors);
    let num_non_null = vectors.len() - vectors.logical_null_count();
    let data = RecordBatch::try_new(data.schema(), vec![vectors]).unwrap();

    let mut dataset = InsertBuilder::new("memory://")
        .execute(vec![data])
        .await
        .unwrap();

    // Create index
    dataset
        .create_index(&["vec"], IndexType::Vector, None, &index_params, false)
        .await
        .unwrap();

    let query = vec![0.0; test_case.dimension]
        .into_iter()
        .collect::<Float32Array>();
    let results = dataset
        .scan()
        .nearest("vec", &query, 2_000)
        .unwrap()
        .ef(100_000)
        .minimum_nprobes(2)
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), num_non_null);
    assert_eq!(results["vec"].logical_null_count(), 0);
}

#[tokio::test]
async fn test_initialize_vector_index_empty_dataset() {
    let test_dir = TempStrDir::default();
    let source_uri = format!("{}/source", test_dir.as_str());
    let target_uri = format!("{}/target", test_dir.as_str());

    // Create source dataset with vector column
    let source_reader = lance_datagen::gen_batch()
        .col("id", array::step::<Int32Type>())
        .col("vector", array::rand_vec::<Float32Type>(32.into()))
        .into_reader_rows(RowCount::from(300), BatchCount::from(1));
    let mut source_dataset = Dataset::write(source_reader, &source_uri, None)
        .await
        .unwrap();

    // Create IVF_PQ index on source
    let params = VectorIndexParams::ivf_pq(10, 8, 16, MetricType::L2, 50);
    source_dataset
        .create_index(
            &["vector"],
            IndexType::Vector,
            Some("vector_ivf_pq".to_string()),
            &params,
            false,
        )
        .await
        .unwrap();

    // Reload to get updated metadata
    let source_dataset = Dataset::open(&source_uri).await.unwrap();
    let source_indices = source_dataset.load_indices().await.unwrap();
    let source_index = source_indices
        .iter()
        .find(|idx| idx.name == "vector_ivf_pq")
        .unwrap();

    // Create EMPTY target dataset with same schema
    let empty_reader = lance_datagen::gen_batch()
        .col("id", array::step::<Int32Type>())
        .col("vector", array::rand_vec::<Float32Type>(32.into()))
        .into_reader_rows(RowCount::from(0), BatchCount::from(1)); // Empty dataset
    let mut target_dataset = Dataset::write(empty_reader, &target_uri, None)
        .await
        .unwrap();

    // Initialize IVF_PQ index on empty target
    initialize_vector_index(
        &mut target_dataset,
        &source_dataset,
        source_index,
        &["vector"],
    )
    .await
    .unwrap();

    // Verify index was created even though dataset is empty
    let target_indices = target_dataset.load_indices().await.unwrap();
    assert_eq!(target_indices.len(), 1, "Empty target should have 1 index");
    assert_eq!(
        target_indices[0].name, "vector_ivf_pq",
        "Index name should match"
    );

    // Open both indices to compare centroids
    let source_vector_index = source_dataset
        .open_vector_index(
            "vector",
            &source_index.uuid.to_string(),
            &NoOpMetricsCollector,
        )
        .await
        .unwrap();

    let target_vector_index = target_dataset
        .open_vector_index(
            "vector",
            &target_indices[0].uuid.to_string(),
            &NoOpMetricsCollector,
        )
        .await
        .unwrap();

    // Get IVF models from both indices
    let source_ivf_model = source_vector_index.ivf_model();
    let target_ivf_model = target_vector_index.ivf_model();

    // Verify they have the same number of partitions
    assert_eq!(
        source_ivf_model.num_partitions(),
        target_ivf_model.num_partitions(),
        "Empty dataset should still have same number of partitions as source"
    );

    // Verify the centroids are exactly the same even for empty dataset
    if let (Some(source_centroids), Some(target_centroids)) =
        (&source_ivf_model.centroids, &target_ivf_model.centroids)
    {
        assert_eq!(
            source_centroids.len(),
            target_centroids.len(),
            "Centroids arrays should have same length even for empty dataset"
        );

        // Compare actual centroid values
        for i in 0..source_centroids.len() {
            let source_centroid = source_centroids.value(i);
            let target_centroid = target_centroids.value(i);

            let source_data = source_centroid
                .as_any()
                .downcast_ref::<arrow_array::PrimitiveArray<arrow_array::types::Float32Type>>()
                .expect("Centroid should be Float32Array");
            let target_data = target_centroid
                .as_any()
                .downcast_ref::<arrow_array::PrimitiveArray<arrow_array::types::Float32Type>>()
                .expect("Centroid should be Float32Array");

            assert_eq!(
                source_data.values(),
                target_data.values(),
                "Empty dataset should have identical centroids from source"
            );
        }
    } else {
        panic!("Both source and empty target should have centroids");
    }

    // Now add data to the target dataset
    let new_data_reader = lance_datagen::gen_batch()
        .col("id", array::step::<Int32Type>())
        .col("vector", array::rand_vec::<Float32Type>(32.into()))
        .into_reader_rows(RowCount::from(100), BatchCount::from(1));
    target_dataset.append(new_data_reader, None).await.unwrap();

    // Run optimize_indices to index the newly added data and merge indices
    // We set num_indices_to_merge to a high value to force merging all indices into one
    use lance_index::optimize::OptimizeOptions;
    target_dataset
        .optimize_indices(&OptimizeOptions::merge(10))
        .await
        .unwrap();

    // Reload dataset to get updated index metadata
    let target_dataset = Dataset::open(&target_uri).await.unwrap();

    // Verify we have only one merged index after optimization
    let index_stats = target_dataset
        .index_statistics("vector_ivf_pq")
        .await
        .unwrap();
    let stats_json: serde_json::Value = serde_json::from_str(&index_stats).unwrap();
    assert_eq!(
        stats_json["num_indices"], 1,
        "Should have only 1 merged index after optimize with high num_indices_to_merge"
    );
    assert_eq!(
        stats_json["num_indexed_fragments"], 1,
        "Should have indexed the appended fragment (empty dataset has no fragments)"
    );
    assert_eq!(
        stats_json["num_unindexed_fragments"], 0,
        "All fragments should be indexed after optimization"
    );

    // The index should now work with the new data
    let query_vector = lance_datagen::gen_batch()
        .anon_col(array::rand_vec::<Float32Type>(32.into()))
        .into_batch_rows(RowCount::from(1))
        .unwrap()
        .column(0)
        .clone();
    let query_vector = query_vector
        .as_any()
        .downcast_ref::<arrow_array::FixedSizeListArray>()
        .unwrap();

    let results = target_dataset
        .scan()
        .nearest("vector", &query_vector.value(0), 5)
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(
        results.num_rows(),
        5,
        "Should return 5 nearest neighbors after optimizing index"
    );

    // Verify that the optimized index still shares centroids with the source
    let target_indices = target_dataset.load_indices().await.unwrap();
    let target_vector_index = target_dataset
        .open_vector_index(
            "vector",
            &target_indices[0].uuid.to_string(),
            &NoOpMetricsCollector,
        )
        .await
        .unwrap();

    let target_ivf_model = target_vector_index.ivf_model();

    // Verify centroids are still the same after optimization
    if let (Some(source_centroids), Some(target_centroids)) =
        (&source_ivf_model.centroids, &target_ivf_model.centroids)
    {
        for i in 0..source_centroids.len() {
            let source_centroid = source_centroids.value(i);
            let target_centroid = target_centroids.value(i);

            let source_data = source_centroid
                .as_any()
                .downcast_ref::<arrow_array::PrimitiveArray<arrow_array::types::Float32Type>>()
                .expect("Centroid should be Float32Array");
            let target_data = target_centroid
                .as_any()
                .downcast_ref::<arrow_array::PrimitiveArray<arrow_array::types::Float32Type>>()
                .expect("Centroid should be Float32Array");

            assert_eq!(
                source_data.values(),
                target_data.values(),
                "Centroids should remain identical after optimize_indices"
            );
        }
    }
}
