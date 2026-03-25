// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow_array::{ArrayRef, Float32Array, Int32Array, RecordBatch, StringArray, UInt32Array};
use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::scanner::ColumnOrdering;
use lance::dataset::{InsertBuilder, WriteMode, WriteParams};
use lance_core::utils::tempfile::TempStrDir;
use lance_index::optimize::OptimizeOptions;
use lance_index::scalar::inverted::query::{FtsQuery, PhraseQuery};
use lance_index::scalar::{FullTextSearchQuery, InvertedIndexParams};
use lance_index::{DatasetIndexExt, IndexType};
use tantivy::tokenizer::Language;

use super::{strip_score_column, test_fts, test_scan, test_take};
use crate::utils::DatasetTestCases;

// Build baseline inverted index parameters for tests, toggling token positions.
fn base_inverted_params(with_position: bool) -> InvertedIndexParams {
    InvertedIndexParams::new("simple".to_string(), Language::English)
        .with_position(with_position)
        .lower_case(true)
        .stem(false)
        .remove_stop_words(false)
        .ascii_folding(false)
        .max_token_length(None)
}

fn params_for(base_tokenizer: &str, lower_case: bool, with_position: bool) -> InvertedIndexParams {
    InvertedIndexParams::new(base_tokenizer.to_string(), Language::English)
        .with_position(with_position)
        .lower_case(lower_case)
        .stem(false)
        .remove_stop_words(false)
        .ascii_folding(false)
        .max_token_length(None)
}

// Execute a full-text search with optional filter and deterministic id ordering.
async fn run_fts(ds: &Dataset, query: FullTextSearchQuery, filter: Option<&str>) -> RecordBatch {
    let mut scanner = ds.scan();
    scanner.full_text_search(query).unwrap();
    if let Some(predicate) = filter {
        scanner.filter(predicate).unwrap();
    }
    scanner
        .order_by(Some(vec![ColumnOrdering::asc_nulls_first(
            "id".to_string(),
        )]))
        .unwrap();
    scanner.try_into_batch().await.unwrap()
}

// Run an FTS query and assert results match a deterministic expected batch.
async fn assert_fts_expected(
    original: &RecordBatch,
    ds: &Dataset,
    query: FullTextSearchQuery,
    filter: Option<&str>,
    expected_ids: &[i32],
) {
    let scanned = run_fts(ds, query, filter).await;
    let scanned = strip_score_column(&scanned, original.schema().as_ref());

    let indices_u32: Vec<u32> = expected_ids.iter().map(|&i| i as u32).collect();
    let indices_array = UInt32Array::from(indices_u32);
    let expected = arrow::compute::take_record_batch(original, &indices_array).unwrap();

    // Ensure ordering is deterministic (id asc) and matches the expected rows.
    assert_eq!(&expected, &scanned);
}

#[tokio::test]
// Ensure indexed and non-indexed full-text search return the same ids.
async fn test_inverted_basic_equivalence() {
    let ids = Arc::new(Int32Array::from((0..10).collect::<Vec<i32>>()));
    let text_values = vec![
        Some("hello world"),
        Some("world hello"),
        Some("hello"),
        Some("lance database"),
        Some(""),
        None,
        Some("hello lance"),
        Some("lance"),
        Some("database"),
        Some("world"),
    ];
    let text = Arc::new(StringArray::from(text_values)) as ArrayRef;
    let batch = RecordBatch::try_from_iter(vec![("id", ids as ArrayRef), ("text", text)]).unwrap();

    DatasetTestCases::from_data(batch.clone())
        .run(|ds, original| async move {
            let mut ds = ds;
            let query = FullTextSearchQuery::new("hello".to_string())
                .with_column("text".to_string())
                .unwrap();

            let expected_ids = vec![0, 1, 2, 6];
            assert_fts_expected(&original, &ds, query.clone(), None, &expected_ids).await;

            let params = base_inverted_params(false);
            ds.create_index(&["text"], IndexType::Inverted, None, &params, true)
                .await
                .unwrap();
            assert_fts_expected(&original, &ds, query.clone(), None, &expected_ids).await;
            test_fts(&original, &ds, "text", "hello", None, true, false).await;

            test_scan(&original, &ds).await;
            test_take(&original, &ds).await;
        })
        .await;
}

#[tokio::test]
// Verify phrase queries require token positions and match contiguous terms.
async fn test_inverted_phrase_query_with_positions() {
    let ids = Arc::new(Int32Array::from((0..6).collect::<Vec<i32>>()));
    let text_values = vec![
        Some("lance database"),
        Some("lance and database"),
        Some("database lance"),
        Some("lance database test"),
        Some("lance database"),
        None,
    ];
    let text = Arc::new(StringArray::from(text_values)) as ArrayRef;
    let batch = RecordBatch::try_from_iter(vec![("id", ids as ArrayRef), ("text", text)]).unwrap();

    DatasetTestCases::from_data(batch.clone())
        .run(|ds, original| async move {
            let mut ds = ds;
            let params = base_inverted_params(true);
            ds.create_index(&["text"], IndexType::Inverted, None, &params, true)
                .await
                .unwrap();

            let phrase = PhraseQuery::new("lance database".to_string())
                .with_column(Some("text".to_string()));
            let query = FullTextSearchQuery::new_query(FtsQuery::Phrase(phrase));

            assert_fts_expected(&original, &ds, query, None, &[0, 3, 4]).await;
            test_fts(&original, &ds, "text", "lance database", None, true, true).await;
        })
        .await;
}

#[tokio::test]
// Validate filters are applied alongside inverted index search results.
async fn test_inverted_with_filter() {
    let ids = Arc::new(Int32Array::from((0..5).collect::<Vec<i32>>()));
    let text_values = vec![
        Some("lance database"),
        Some("lance vector"),
        Some("random text"),
        Some("lance"),
        None,
    ];
    let categories = vec![
        Some("keep"),
        Some("drop"),
        Some("keep"),
        Some("keep"),
        Some("keep"),
    ];
    let text = Arc::new(StringArray::from(text_values)) as ArrayRef;
    let category = Arc::new(StringArray::from(categories)) as ArrayRef;
    let batch = RecordBatch::try_from_iter(vec![
        ("id", ids as ArrayRef),
        ("text", text),
        ("category", category),
    ])
    .unwrap();

    DatasetTestCases::from_data(batch.clone())
        .with_index_types(
            "category",
            [
                None,
                Some(IndexType::Bitmap),
                Some(IndexType::BTree),
                Some(IndexType::BloomFilter),
                Some(IndexType::ZoneMap),
            ],
        )
        .run(|ds, original| async move {
            let mut ds = ds;
            let params = base_inverted_params(false);
            ds.create_index(&["text"], IndexType::Inverted, None, &params, true)
                .await
                .unwrap();

            let query = FullTextSearchQuery::new("lance".to_string())
                .with_column("text".to_string())
                .unwrap();
            assert_fts_expected(&original, &ds, query, Some("category = 'keep'"), &[0, 3]).await;
            test_fts(
                &original,
                &ds,
                "text",
                "lance",
                Some("category = 'keep'"),
                true,
                false,
            )
            .await;
        })
        .await;
}

#[tokio::test]
// Validate tokenizer/lowercase/position parameter combinations against expected matches.
async fn test_inverted_params_combinations() {
    let ids = Arc::new(Int32Array::from((0..5).collect::<Vec<i32>>()));
    let text_values = vec![
        Some("Hello there, this is a longer sentence about Lance."),
        Some("In this longer sentence we say hello to the database."),
        Some("Another line: hello world appears in a longer phrase."),
        Some("Saying HELLO loudly in a long sentence for testing."),
        None,
    ];
    let text = Arc::new(StringArray::from(text_values)) as ArrayRef;
    let batch = RecordBatch::try_from_iter(vec![("id", ids as ArrayRef), ("text", text)]).unwrap();

    let cases = vec![
        (
            "simple_lc_pos",
            params_for("simple", true, true),
            vec![0, 1, 2, 3],
            true,
        ),
        (
            "simple_no_lc",
            params_for("simple", false, false),
            vec![1, 2],
            false,
        ),
        (
            "whitespace_lc",
            params_for("whitespace", true, false),
            vec![0, 1, 2, 3],
            true,
        ),
        (
            "whitespace_no_lc_pos",
            params_for("whitespace", false, true),
            vec![1, 2],
            false,
        ),
    ];

    for (_name, params, expected, lower_case) in cases {
        let params = params.clone();
        let expected = expected.clone();
        DatasetTestCases::from_data(batch.clone())
            .with_index_types_and_inverted_index_params("text", [Some(IndexType::Inverted)], params)
            .run(|ds, original| {
                let expected = expected.clone();
                async move {
                    let query = FullTextSearchQuery::new("hello".to_string())
                        .with_column("text".to_string())
                        .unwrap();
                    assert_fts_expected(&original, &ds, query.clone(), None, &expected).await;
                    test_fts(&original, &ds, "text", "hello", None, lower_case, false).await;
                }
            })
            .await;
    }
}

/// Regression test: FTS query after deleting rows should not crash with
/// "Attempt to merge two RecordBatch with different sizes".
///
/// When stable row IDs are enabled, the FTS index may return row IDs for
/// deleted rows. The row ID index excludes deleted rows, so get_row_addrs()
/// must filter the input batch to match. Without this filtering, the
/// downstream merge in TakeExec fails with a size mismatch.
#[tokio::test]
async fn test_fts_after_delete_with_stable_row_ids() {
    let ids = Arc::new(Int32Array::from((0..20).collect::<Vec<i32>>()));
    // Give each row a unique word + a common word "shared"
    let texts: Vec<Option<&str>> = (0..20)
        .map(|i| match i % 4 {
            0 => Some("alpha shared"),
            1 => Some("beta shared"),
            2 => Some("gamma shared"),
            _ => Some("delta shared"),
        })
        .collect();
    let text_col = Arc::new(StringArray::from(texts));
    let batch = RecordBatch::try_from_iter(vec![
        ("id", ids as ArrayRef),
        ("text", text_col as ArrayRef),
    ])
    .unwrap();

    // Create dataset with stable row IDs
    let mut ds = InsertBuilder::new("memory://")
        .with_params(&WriteParams {
            enable_stable_row_ids: true,
            ..Default::default()
        })
        .execute(vec![batch])
        .await
        .unwrap();

    // Create FTS index
    let params = InvertedIndexParams::default();
    ds.create_index_builder(&["text"], IndexType::Inverted, &params)
        .await
        .unwrap();

    // Delete some rows — these will still be referenced by the FTS index
    ds.delete("id IN (0, 1, 2, 3, 4)").await.unwrap();

    // FTS query for "shared" — matches ALL rows including deleted ones.
    // Before the fix, this would crash with a merge size mismatch.
    let query = FullTextSearchQuery::new("shared".to_string())
        .with_column("text".to_string())
        .unwrap();
    let mut scanner = ds.scan();
    scanner.full_text_search(query).unwrap();
    scanner
        .order_by(Some(vec![ColumnOrdering::asc_nulls_first(
            "id".to_string(),
        )]))
        .unwrap();
    let result = scanner.try_into_batch().await.unwrap();

    // Should only have 15 rows (20 - 5 deleted)
    assert_eq!(result.num_rows(), 15);

    // Verify no deleted IDs are present
    let result_ids = result
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    for id in result_ids.values().iter() {
        assert!(*id >= 5, "Deleted row id {} should not appear", id);
    }
}

// Helper: create a dataset with text data, split into initial + append batches.
async fn make_fts_dataset_with_append(test_uri: &str, with_position: bool) -> Dataset {
    let ids1: Vec<i32> = (0..10).collect();
    let texts1 = vec![
        "hello world",
        "world hello",
        "hello",
        "lance database",
        "search engine",
        "hello lance",
        "lance",
        "database",
        "world",
        "hello world search",
    ];

    let batch1 = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(ids1)) as ArrayRef),
        (
            "text",
            Arc::new(StringArray::from(
                texts1.into_iter().map(Some).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
    ])
    .unwrap();

    let mut ds = InsertBuilder::new(test_uri)
        .execute(vec![batch1])
        .await
        .unwrap();

    let params = base_inverted_params(with_position);
    ds.create_index_builder(&["text"], IndexType::Inverted, &params)
        .await
        .unwrap();

    // Append more data
    let ids2: Vec<i32> = (10..20).collect();
    let texts2 = vec![
        "hello database",
        "search world",
        "lance search engine",
        "hello hello hello",
        "world world",
        "database search",
        "hello engine",
        "lance world",
        "search hello",
        "engine database lance",
    ];

    let batch2 = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(ids2)) as ArrayRef),
        (
            "text",
            Arc::new(StringArray::from(
                texts2.into_iter().map(Some).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
    ])
    .unwrap();

    let append_params = WriteParams {
        mode: WriteMode::Append,
        ..Default::default()
    };
    InsertBuilder::new(test_uri)
        .with_params(&append_params)
        .execute(vec![batch2])
        .await
        .unwrap();

    DatasetBuilder::from_uri(test_uri).load().await.unwrap()
}

// Run FTS and return (row_ids, scores) sorted by row_id for deterministic comparison.
async fn fts_results_sorted(ds: &Dataset, query: &str) -> Vec<(i32, f32)> {
    let fts_query = FullTextSearchQuery::new(query.to_string())
        .with_column("text".to_string())
        .unwrap();
    let mut scanner = ds.scan();
    scanner.full_text_search(fts_query).unwrap();
    scanner
        .order_by(Some(vec![ColumnOrdering::asc_nulls_first(
            "id".to_string(),
        )]))
        .unwrap();
    let batch = scanner.try_into_batch().await.unwrap();
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let scores = batch
        .column_by_name("_score")
        .unwrap()
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    ids.values()
        .iter()
        .zip(scores.values().iter())
        .map(|(&id, &score)| (id, score))
        .collect()
}

#[tokio::test]
async fn test_fts_append_creates_separate_segment() {
    let test_dir = TempStrDir::default();
    let mut ds = make_fts_dataset_with_append(test_dir.as_str(), false).await;

    let indices = ds.load_indices().await.unwrap();
    let fts_idx_name = indices
        .iter()
        .find(|i| i.name.contains("text"))
        .unwrap()
        .name
        .clone();

    // Before optimize: only 1 segment
    assert_eq!(
        ds.load_indices_by_name(&fts_idx_name).await.unwrap().len(),
        1
    );

    // Append mode: create a new segment without merging
    ds.optimize_indices(&OptimizeOptions::append())
        .await
        .unwrap();
    let ds = DatasetBuilder::from_uri(test_dir.as_str())
        .load()
        .await
        .unwrap();

    // Should have 2 segments
    let segments = ds.load_indices_by_name(&fts_idx_name).await.unwrap();
    assert_eq!(segments.len(), 2, "Expected 2 FTS segments after append");

    // Fragment bitmaps should be disjoint
    let bm0 = segments[0].fragment_bitmap.as_ref().unwrap();
    let bm1 = segments[1].fragment_bitmap.as_ref().unwrap();
    assert!(
        bm0.intersection_len(bm1) == 0,
        "Segment fragment bitmaps should be disjoint"
    );
}

#[tokio::test]
async fn test_fts_multi_segment_score_equivalence() {
    // Build single-segment index on full data
    let single_dir = TempStrDir::default();
    let mut ds_single = make_fts_dataset_with_append(single_dir.as_str(), false).await;

    // Rebuild index on the whole dataset (single segment)
    ds_single
        .optimize_indices(&OptimizeOptions::default())
        .await
        .unwrap();
    let ds_single = DatasetBuilder::from_uri(single_dir.as_str())
        .load()
        .await
        .unwrap();

    // Build multi-segment index on same data
    let multi_dir = TempStrDir::default();
    let mut ds_multi = make_fts_dataset_with_append(multi_dir.as_str(), false).await;

    // Append mode: create second segment
    ds_multi
        .optimize_indices(&OptimizeOptions::append())
        .await
        .unwrap();
    let ds_multi = DatasetBuilder::from_uri(multi_dir.as_str())
        .load()
        .await
        .unwrap();

    // Verify 2 segments
    let indices = ds_multi.load_indices().await.unwrap();
    let fts_name = indices
        .iter()
        .find(|i| i.name.contains("text"))
        .unwrap()
        .name
        .clone();
    let segments = ds_multi.load_indices_by_name(&fts_name).await.unwrap();
    assert_eq!(segments.len(), 2);

    // Compare results for multiple queries
    for query in &["hello", "world", "lance", "database", "search", "engine"] {
        let single_results = fts_results_sorted(&ds_single, query).await;
        let multi_results = fts_results_sorted(&ds_multi, query).await;

        assert_eq!(
            single_results.len(),
            multi_results.len(),
            "Result count mismatch for query '{}'",
            query
        );

        for (i, ((s_id, s_score), (m_id, m_score))) in
            single_results.iter().zip(multi_results.iter()).enumerate()
        {
            assert_eq!(
                s_id, m_id,
                "ID mismatch at position {} for query '{}'",
                i, query
            );
            assert!(
                (s_score - m_score).abs() < 1e-5,
                "Score mismatch at position {} for query '{}': single={}, multi={}",
                i,
                query,
                s_score,
                m_score,
            );
        }
    }
}

#[tokio::test]
async fn test_fts_merge_combines_segments() {
    let test_dir = TempStrDir::default();
    let mut ds = make_fts_dataset_with_append(test_dir.as_str(), false).await;

    // Create second segment
    ds.optimize_indices(&OptimizeOptions::append())
        .await
        .unwrap();
    let ds = DatasetBuilder::from_uri(test_dir.as_str())
        .load()
        .await
        .unwrap();

    let indices = ds.load_indices().await.unwrap();
    let fts_name = indices
        .iter()
        .find(|i| i.name.contains("text"))
        .unwrap()
        .name
        .clone();
    assert_eq!(ds.load_indices_by_name(&fts_name).await.unwrap().len(), 2);

    // Query before merge
    let before_merge = fts_results_sorted(&ds, "hello").await;

    // Merge all segments
    let mut ds = ds;
    ds.optimize_indices(&OptimizeOptions::merge(2))
        .await
        .unwrap();
    let ds = DatasetBuilder::from_uri(test_dir.as_str())
        .load()
        .await
        .unwrap();

    // Should be back to 1 segment
    let segments = ds.load_indices_by_name(&fts_name).await.unwrap();
    assert_eq!(segments.len(), 1, "Expected 1 segment after merge(2)");

    // Results should be the same
    let after_merge = fts_results_sorted(&ds, "hello").await;
    assert_eq!(
        before_merge.len(),
        after_merge.len(),
        "Result count changed after merge"
    );
    for ((_, s1), (_, s2)) in before_merge.iter().zip(after_merge.iter()) {
        assert!(
            (s1 - s2).abs() < 1e-5,
            "Scores changed after merge: {} vs {}",
            s1,
            s2,
        );
    }
}

#[tokio::test]
async fn test_fts_three_segments_partial_merge() {
    let test_dir = TempStrDir::default();

    // Create initial data
    let batch1 = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(vec![0, 1, 2])) as ArrayRef),
        (
            "text",
            Arc::new(StringArray::from(vec!["hello world", "lance db", "search"])) as ArrayRef,
        ),
    ])
    .unwrap();

    let mut ds = InsertBuilder::new(test_dir.as_str())
        .execute(vec![batch1])
        .await
        .unwrap();

    let params = base_inverted_params(false);
    ds.create_index_builder(&["text"], IndexType::Inverted, &params)
        .await
        .unwrap();

    // Append batch 2 and create segment
    let batch2 = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(vec![3, 4, 5])) as ArrayRef),
        (
            "text",
            Arc::new(StringArray::from(vec!["hello lance", "world search", "db"])) as ArrayRef,
        ),
    ])
    .unwrap();
    let append_params = WriteParams {
        mode: WriteMode::Append,
        ..Default::default()
    };
    InsertBuilder::new(test_dir.as_str())
        .with_params(&append_params)
        .execute(vec![batch2])
        .await
        .unwrap();
    let mut ds = DatasetBuilder::from_uri(test_dir.as_str())
        .load()
        .await
        .unwrap();
    ds.optimize_indices(&OptimizeOptions::append())
        .await
        .unwrap();

    // Append batch 3 and create segment
    let batch3 = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(vec![6, 7, 8])) as ArrayRef),
        (
            "text",
            Arc::new(StringArray::from(vec![
                "hello search",
                "lance world",
                "db engine",
            ])) as ArrayRef,
        ),
    ])
    .unwrap();
    InsertBuilder::new(test_dir.as_str())
        .with_params(&append_params)
        .execute(vec![batch3])
        .await
        .unwrap();
    let mut ds = DatasetBuilder::from_uri(test_dir.as_str())
        .load()
        .await
        .unwrap();
    ds.optimize_indices(&OptimizeOptions::append())
        .await
        .unwrap();
    let ds = DatasetBuilder::from_uri(test_dir.as_str())
        .load()
        .await
        .unwrap();

    let indices = ds.load_indices().await.unwrap();
    let fts_name = indices
        .iter()
        .find(|i| i.name.contains("text"))
        .unwrap()
        .name
        .clone();
    assert_eq!(ds.load_indices_by_name(&fts_name).await.unwrap().len(), 3);

    // Query across 3 segments
    let results_3seg = fts_results_sorted(&ds, "hello").await;
    assert_eq!(results_3seg.len(), 3); // rows 0, 3, 6

    // Partial merge: merge 2 most recent
    let mut ds = ds;
    ds.optimize_indices(&OptimizeOptions::merge(2))
        .await
        .unwrap();
    let ds = DatasetBuilder::from_uri(test_dir.as_str())
        .load()
        .await
        .unwrap();

    // Should now have 2 segments (original + merged)
    let segments = ds.load_indices_by_name(&fts_name).await.unwrap();
    assert_eq!(segments.len(), 2, "Expected 2 segments after merge(2)");

    // Results should be the same
    let results_2seg = fts_results_sorted(&ds, "hello").await;
    assert_eq!(results_3seg.len(), results_2seg.len());
}

#[tokio::test]
async fn test_fts_multi_segment_with_unindexed_data() {
    let test_dir = TempStrDir::default();
    let mut ds = make_fts_dataset_with_append(test_dir.as_str(), false).await;

    // Create second segment
    ds.optimize_indices(&OptimizeOptions::append())
        .await
        .unwrap();

    // Append more data (stays unindexed)
    let batch3 = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(vec![20, 21])) as ArrayRef),
        (
            "text",
            Arc::new(StringArray::from(vec![
                "hello unindexed",
                "world unindexed",
            ])) as ArrayRef,
        ),
    ])
    .unwrap();
    let append_params = WriteParams {
        mode: WriteMode::Append,
        ..Default::default()
    };
    InsertBuilder::new(test_dir.as_str())
        .with_params(&append_params)
        .execute(vec![batch3])
        .await
        .unwrap();
    let ds = DatasetBuilder::from_uri(test_dir.as_str())
        .load()
        .await
        .unwrap();

    // Query should include results from 2 segments + unindexed flat scan
    let results = fts_results_sorted(&ds, "hello").await;
    // Should find "hello" in indexed segments and in unindexed data
    assert!(
        results.iter().any(|(id, _)| *id == 20),
        "Should find results in unindexed data"
    );
    assert!(
        results.iter().any(|(id, _)| *id == 0),
        "Should find results in first segment"
    );
    assert!(
        results.iter().any(|(id, _)| *id == 10),
        "Should find results in second segment"
    );
}

#[tokio::test]
async fn test_fts_multi_segment_phrase_query() {
    let test_dir = TempStrDir::default();
    let mut ds = make_fts_dataset_with_append(test_dir.as_str(), true).await;

    // Create second segment (with positions)
    ds.optimize_indices(&OptimizeOptions::append())
        .await
        .unwrap();
    let ds = DatasetBuilder::from_uri(test_dir.as_str())
        .load()
        .await
        .unwrap();

    // Phrase query across 2 segments
    let phrase_query =
        PhraseQuery::new("hello world".to_string()).with_column(Some("text".to_string()));
    let fts_query = FullTextSearchQuery::new_query(phrase_query.into());
    let mut scanner = ds.scan();
    scanner.full_text_search(fts_query).unwrap();
    scanner
        .order_by(Some(vec![ColumnOrdering::asc_nulls_first(
            "id".to_string(),
        )]))
        .unwrap();
    let batch = scanner.try_into_batch().await.unwrap();

    // "hello world" appears at rows 0 and 9
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let id_values: Vec<i32> = ids.values().to_vec();
    assert!(
        id_values.contains(&0),
        "Should find 'hello world' in first segment"
    );
    assert!(
        id_values.contains(&9),
        "'hello world search' should match phrase 'hello world'"
    );
}
