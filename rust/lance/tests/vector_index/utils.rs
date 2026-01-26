// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Shared test utilities for vector index integration tests.

use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::{Float32Type, UInt64Type};
use arrow_array::{
    Array, ArrayRef, ArrowPrimitiveType, FixedSizeListArray, RecordBatch, RecordBatchIterator,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use lance::dataset::WriteParams;
use lance::Dataset;
use lance_arrow::FixedSizeListArrayExt;
use lance_core::ROW_ID;
use lance_linalg::distance::DistanceType;
use lance_linalg::kernels::normalize_fsl;
use lance_testing::datagen::generate_random_array_with_range;
use rand::distr::uniform::SampleUniform;

pub const NUM_ROWS: usize = 512;
pub const DIM: usize = 32;

pub async fn generate_test_dataset<T: ArrowPrimitiveType>(
    test_uri: &str,
    range: Range<T::Native>,
) -> (Dataset, Arc<FixedSizeListArray>)
where
    T::Native: SampleUniform,
{
    let (batch, schema) = generate_batch::<T>(NUM_ROWS, None, range, false);
    let vectors = batch.column_by_name("vector").unwrap().clone();
    let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    let dataset = Dataset::write(
        batches,
        test_uri,
        Some(WriteParams {
            mode: lance::dataset::WriteMode::Overwrite,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    (
        dataset,
        Arc::new(
            vectors
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .unwrap()
                .clone(),
        ),
    )
}

pub async fn append_dataset<T: ArrowPrimitiveType>(
    dataset: &mut Dataset,
    num_rows: usize,
    range: Range<T::Native>,
) -> ArrayRef
where
    T::Native: SampleUniform,
{
    let is_multivector = matches!(
        dataset.schema().field("vector").unwrap().data_type(),
        DataType::List(_)
    );
    let row_count = dataset.count_rows(None).await.unwrap();
    let (batch, schema) =
        generate_batch::<T>(num_rows, Some(row_count as u64), range, is_multivector);
    let vectors = batch["vector"].clone();
    let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    dataset.append(batches, None).await.unwrap();
    vectors
}

pub fn generate_batch<T: ArrowPrimitiveType>(
    num_rows: usize,
    start_id: Option<u64>,
    range: Range<T::Native>,
    is_multivector: bool,
) -> (RecordBatch, SchemaRef)
where
    T::Native: SampleUniform,
{
    const VECTOR_NUM_PER_ROW: usize = 3;
    let start_id = start_id.unwrap_or(0);
    let ids = Arc::new(UInt64Array::from_iter_values(
        start_id..start_id + num_rows as u64,
    ));
    let total_floats = match is_multivector {
        true => num_rows * VECTOR_NUM_PER_ROW * DIM,
        false => num_rows * DIM,
    };
    let vectors = generate_random_array_with_range::<T>(total_floats, range);
    let data_type = vectors.data_type().clone();
    let mut fields = vec![Field::new("id", DataType::UInt64, false)];
    let mut arrays: Vec<ArrayRef> = vec![ids];
    let mut fsl = FixedSizeListArray::try_new_from_values(vectors, DIM as i32).unwrap();
    if fsl.value_type() != DataType::UInt8 {
        fsl = normalize_fsl(&fsl).unwrap();
    }
    if is_multivector {
        use arrow_array::ListArray;
        use arrow_buffer::OffsetBuffer;

        let vector_field = Arc::new(Field::new(
            "item",
            DataType::FixedSizeList(Arc::new(Field::new("item", data_type, true)), DIM as i32),
            true,
        ));
        fields.push(Field::new(
            "vector",
            DataType::List(vector_field.clone()),
            true,
        ));
        let array = Arc::new(ListArray::new(
            vector_field,
            OffsetBuffer::from_lengths(std::iter::repeat_n(VECTOR_NUM_PER_ROW, num_rows)),
            Arc::new(fsl),
            None,
        ));
        arrays.push(array);
    } else {
        fields.push(Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", data_type, true)), DIM as i32),
            true,
        ));
        let array = Arc::new(fsl);
        arrays.push(array);
    }
    let schema: Arc<_> = Schema::new(fields).into();
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    (batch, schema)
}

pub async fn ground_truth(
    dataset: &Dataset,
    column: &str,
    query: &dyn Array,
    k: usize,
    distance_type: DistanceType,
) -> HashSet<u64> {
    let batch = dataset
        .scan()
        .with_row_id()
        .nearest(column, query, k)
        .unwrap()
        .distance_metric(distance_type)
        .use_index(false)
        .try_into_batch()
        .await
        .unwrap();
    batch[ROW_ID]
        .as_primitive::<UInt64Type>()
        .values()
        .iter()
        .copied()
        .collect()
}

pub async fn test_recall<T: ArrowPrimitiveType>(
    nlist: usize,
    recall_requirement: f32,
    vector_column: &str,
    dataset: &Dataset,
    vectors: Arc<FixedSizeListArray>,
    metric_type: DistanceType,
) {
    use arrow::array::AsArray;
    use lance_index::vector::DIST_COL;

    let query = vectors.value(0);
    let k = 100;
    let result = dataset
        .scan()
        .nearest(vector_column, query.as_primitive::<T>(), k)
        .unwrap()
        .nprobes(nlist)
        .with_row_id()
        .try_into_batch()
        .await
        .unwrap();

    let row_ids = result[ROW_ID]
        .as_primitive::<UInt64Type>()
        .values()
        .to_vec();
    let dists = result[DIST_COL]
        .as_primitive::<Float32Type>()
        .values()
        .to_vec();
    let results = dists
        .into_iter()
        .zip(row_ids.into_iter())
        .collect::<Vec<_>>();
    let row_ids = results.iter().map(|(_, id)| *id).collect::<HashSet<_>>();
    assert!(row_ids.len() == k);

    let gt = ground_truth(dataset, vector_column, &query, k, metric_type).await;

    let recall = row_ids.intersection(&gt).count() as f32 / k as f32;
    assert!(
        recall >= recall_requirement,
        "recall: {}\n results: {:?}\n\ngt: {:?}",
        recall,
        results,
        gt,
    );
}
