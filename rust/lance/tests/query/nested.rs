// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow_array::{
    builder::{Int32Builder, ListBuilder, StringBuilder, StructBuilder},
    ArrayRef, Int32Array, RecordBatch, StructArray,
};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_schema::{DataType, Field, Fields};
use lance::Dataset;
use lance_index::IndexType;

use super::{test_filter, test_scan, test_take};
use crate::utils::DatasetTestCases;

#[tokio::test]
async fn test_query_list_str() {
    let mut builder = ListBuilder::new(StringBuilder::new());

    // 0: ["a", "b"]
    builder.values().append_value("a");
    builder.values().append_value("b");
    builder.append(true);

    // 1: ["c", "d"]
    builder.values().append_value("c");
    builder.values().append_value("d");
    builder.append(true);

    // 2: ["a", "c"]
    builder.values().append_value("a");
    builder.values().append_value("c");
    builder.append(true);

    // 3: ["a", null, "b"] — null element
    builder.values().append_value("a");
    builder.values().append_null();
    builder.values().append_value("b");
    builder.append(true);

    // 4: null — fully null list
    builder.append(false);

    // 5: [] — empty list
    builder.append(true);

    // 6: ["d", "d"] — duplicates
    builder.values().append_value("d");
    builder.values().append_value("d");
    builder.append(true);

    // 7: ["a"] — single element
    builder.values().append_value("a");
    builder.append(true);

    // 8: [null] — list with only null
    builder.values().append_null();
    builder.append(true);

    // 9: ["b", "c", "d"]
    builder.values().append_value("b");
    builder.values().append_value("c");
    builder.values().append_value("d");
    builder.append(true);

    let value_array: ArrayRef = Arc::new(builder.finish());
    let id_array: ArrayRef = Arc::new(Int32Array::from((0..10).collect::<Vec<i32>>()));

    let batch = RecordBatch::try_from_iter(vec![("id", id_array), ("value", value_array)]).unwrap();

    DatasetTestCases::from_data(batch)
        .with_index_types("value", [None, Some(IndexType::LabelList)])
        .run(|ds: Dataset, original: RecordBatch| async move {
            test_scan(&original, &ds).await;
            test_take(&original, &ds).await;
            test_filter(&original, &ds, "array_has_any(value, make_array('a', 'c'))").await;
            test_filter(
                &original,
                &ds,
                "NOT array_has_any(value, make_array('a', 'c'))",
            )
            .await;
            test_filter(&original, &ds, "array_has_all(value, make_array('a', 'b'))").await;
            test_filter(&original, &ds, "array_contains(value, 'a')").await;
            test_filter(
                &original,
                &ds,
                "array_contains(value, 'a') OR array_contains(value, 'd')",
            )
            .await;
            test_filter(&original, &ds, "value is null").await;
            test_filter(&original, &ds, "value is not null").await;
        })
        .await
}

#[tokio::test]
async fn test_query_list_int() {
    let mut builder = ListBuilder::new(Int32Builder::new());

    // 0: [1, 2, 3]
    builder.values().append_value(1);
    builder.values().append_value(2);
    builder.values().append_value(3);
    builder.append(true);

    // 1: [4, 5]
    builder.values().append_value(4);
    builder.values().append_value(5);
    builder.append(true);

    // 2: [1, 4]
    builder.values().append_value(1);
    builder.values().append_value(4);
    builder.append(true);

    // 3: [2, null, 5] — null element
    builder.values().append_value(2);
    builder.values().append_null();
    builder.values().append_value(5);
    builder.append(true);

    // 4: null — fully null list
    builder.append(false);

    // 5: [] — empty list
    builder.append(true);

    // 6: [3, 3, 3] — repeated
    builder.values().append_value(3);
    builder.values().append_value(3);
    builder.values().append_value(3);
    builder.append(true);

    // 7: [1] — single
    builder.values().append_value(1);
    builder.append(true);

    // 8: [null] — only null element
    builder.values().append_null();
    builder.append(true);

    // 9: [2, 4, 6]
    builder.values().append_value(2);
    builder.values().append_value(4);
    builder.values().append_value(6);
    builder.append(true);

    let value_array: ArrayRef = Arc::new(builder.finish());
    let id_array: ArrayRef = Arc::new(Int32Array::from((0..10).collect::<Vec<i32>>()));

    let batch = RecordBatch::try_from_iter(vec![("id", id_array), ("value", value_array)]).unwrap();

    DatasetTestCases::from_data(batch)
        .with_index_types("value", [None, Some(IndexType::LabelList)])
        .run(|ds: Dataset, original: RecordBatch| async move {
            test_scan(&original, &ds).await;
            test_take(&original, &ds).await;
            test_filter(&original, &ds, "array_has_any(value, make_array(1, 4))").await;
            test_filter(&original, &ds, "NOT array_has_any(value, make_array(1, 4))").await;
            test_filter(&original, &ds, "array_has_all(value, make_array(1, 2))").await;
            test_filter(&original, &ds, "array_contains(value, 3)").await;
            test_filter(
                &original,
                &ds,
                "array_contains(value, 1) OR array_contains(value, 5)",
            )
            .await;
            test_filter(&original, &ds, "value is null").await;
            test_filter(&original, &ds, "value is not null").await;
        })
        .await
}

#[tokio::test]
async fn test_query_struct() {
    let name_field = Arc::new(Field::new("name", DataType::Utf8, true));
    let score_field = Arc::new(Field::new("score", DataType::Int32, true));
    let fields = Fields::from(vec![name_field.clone(), score_field.clone()]);

    let names = Arc::new(arrow_array::StringArray::from(vec![
        Some("alice"),
        Some("bob"),
        Some("alice"),
        Some("carol"),
        None, // row 4: entire struct is null
        Some("david"),
        None, // row 6: null name sub-field
        Some("eve"),
        Some("bob"),
        Some("alice"),
    ])) as ArrayRef;

    let scores = Arc::new(Int32Array::from(vec![
        Some(10),
        Some(20),
        Some(30),
        Some(40),
        None, // row 4: entire struct is null
        Some(50),
        Some(60),
        None, // row 7: null score sub-field
        Some(80),
        Some(90),
    ])) as ArrayRef;

    // Row 4 is a fully null struct
    let null_buffer = NullBuffer::new(BooleanBuffer::from(vec![
        true, true, true, true, false, true, true, true, true, true,
    ]));

    let struct_array =
        StructArray::try_new(fields, vec![names, scores], Some(null_buffer)).unwrap();

    let value_array: ArrayRef = Arc::new(struct_array);
    let id_array: ArrayRef = Arc::new(Int32Array::from((0..10).collect::<Vec<i32>>()));

    let batch = RecordBatch::try_from_iter(vec![("id", id_array), ("value", value_array)]).unwrap();

    DatasetTestCases::from_data(batch)
        .with_index_types(
            "value.score",
            [None, Some(IndexType::BTree), Some(IndexType::Bitmap)],
        )
        .run(|ds: Dataset, original: RecordBatch| async move {
            test_scan(&original, &ds).await;
            test_take(&original, &ds).await;
            test_filter(&original, &ds, "value.score > 30").await;
            test_filter(&original, &ds, "NOT (value.score > 30)").await;
            test_filter(&original, &ds, "value.name = 'alice'").await;
            test_filter(&original, &ds, "value.name = 'alice' OR value.score > 70").await;
            test_filter(&original, &ds, "value is null").await;
            test_filter(&original, &ds, "value is not null").await;
            test_filter(&original, &ds, "value.score is null").await;
            test_filter(&original, &ds, "value.name is null").await;
        })
        .await
}

#[tokio::test]
async fn test_query_list_struct() {
    let tag_field = Arc::new(Field::new("tag", DataType::Utf8, true));
    let struct_fields = Fields::from(vec![tag_field.clone()]);

    let mut builder = ListBuilder::new(StructBuilder::from_fields(struct_fields.clone(), 0));

    // 0: [{tag: "a"}, {tag: "b"}]
    builder
        .values()
        .field_builder::<StringBuilder>(0)
        .unwrap()
        .append_value("a");
    builder.values().append(true);
    builder
        .values()
        .field_builder::<StringBuilder>(0)
        .unwrap()
        .append_value("b");
    builder.values().append(true);
    builder.append(true);

    // 1: [{tag: "c"}]
    builder
        .values()
        .field_builder::<StringBuilder>(0)
        .unwrap()
        .append_value("c");
    builder.values().append(true);
    builder.append(true);

    // 2: null — fully null list
    builder.append(false);

    // 3: [] — empty list
    builder.append(true);

    // 4: [{tag: "a"}, {tag: null}] — null in struct field
    builder
        .values()
        .field_builder::<StringBuilder>(0)
        .unwrap()
        .append_value("a");
    builder.values().append(true);
    builder
        .values()
        .field_builder::<StringBuilder>(0)
        .unwrap()
        .append_null();
    builder.values().append(true);
    builder.append(true);

    // 5: [{tag: "d"}, {tag: "e"}, {tag: "f"}]
    builder
        .values()
        .field_builder::<StringBuilder>(0)
        .unwrap()
        .append_value("d");
    builder.values().append(true);
    builder
        .values()
        .field_builder::<StringBuilder>(0)
        .unwrap()
        .append_value("e");
    builder.values().append(true);
    builder
        .values()
        .field_builder::<StringBuilder>(0)
        .unwrap()
        .append_value("f");
    builder.values().append(true);
    builder.append(true);

    // 6: [null, {tag: "g"}] — null struct element in list
    builder
        .values()
        .field_builder::<StringBuilder>(0)
        .unwrap()
        .append_null();
    builder.values().append(false);
    builder
        .values()
        .field_builder::<StringBuilder>(0)
        .unwrap()
        .append_value("g");
    builder.values().append(true);
    builder.append(true);

    // 7: [{tag: "h"}]
    builder
        .values()
        .field_builder::<StringBuilder>(0)
        .unwrap()
        .append_value("h");
    builder.values().append(true);
    builder.append(true);

    // 8: [{tag: "a"}, {tag: "a"}] — duplicate
    builder
        .values()
        .field_builder::<StringBuilder>(0)
        .unwrap()
        .append_value("a");
    builder.values().append(true);
    builder
        .values()
        .field_builder::<StringBuilder>(0)
        .unwrap()
        .append_value("a");
    builder.values().append(true);
    builder.append(true);

    // 9: [{tag: "b"}]
    builder
        .values()
        .field_builder::<StringBuilder>(0)
        .unwrap()
        .append_value("b");
    builder.values().append(true);
    builder.append(true);

    let value_array: ArrayRef = Arc::new(builder.finish());
    let id_array: ArrayRef = Arc::new(Int32Array::from((0..10).collect::<Vec<i32>>()));

    let batch = RecordBatch::try_from_iter(vec![("id", id_array), ("value", value_array)]).unwrap();

    // No index — LabelList doesn't support struct elements
    DatasetTestCases::from_data(batch)
        .run(|ds: Dataset, original: RecordBatch| async move {
            test_scan(&original, &ds).await;
            test_take(&original, &ds).await;
            test_filter(&original, &ds, "value is null").await;
            test_filter(&original, &ds, "value is not null").await;
        })
        .await
}
