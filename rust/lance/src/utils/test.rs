// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use lance_core::utils::tempfile::{TempDir, TempStrDir};

use arrow_array::{RecordBatch, RecordBatchIterator};
use arrow_schema::Schema as ArrowSchema;
use datafusion_physical_plan::ExecutionPlan;
use lance_arrow::RecordBatchExt;
use lance_core::datatypes::Schema;
use lance_datagen::{BatchCount, BatchGeneratorBuilder, ByteCount, RowCount};
use lance_file::version::LanceFileVersion;
use lance_table::format::Fragment;
use rand::prelude::SliceRandom;
use rand::{Rng, SeedableRng};

use crate::dataset::fragment::write::FragmentCreateBuilder;
use crate::dataset::transaction::Operation;
use crate::dataset::WriteParams;
use crate::Dataset;

mod throttle_store;

pub use throttle_store::ThrottledStoreWrapper;

/// A dataset generator that can generate random layouts. This is used to test
/// dataset operations are robust to different layouts.
///
/// "Layout" includes: How the fields are split across files within the same
/// fragment, the order of the field ids, and the order of fields across files.
pub struct TestDatasetGenerator {
    seed: Option<u64>,
    data: Vec<RecordBatch>,
    data_storage_version: LanceFileVersion,
}

impl TestDatasetGenerator {
    /// Create a new dataset generator with the given data.
    ///
    /// Each batch will become a separate fragment in the dataset.
    pub fn new(data: Vec<RecordBatch>, data_storage_version: LanceFileVersion) -> Self {
        assert!(!data.is_empty());
        Self {
            data,
            seed: None,
            data_storage_version,
        }
    }

    /// Set the seed for the random number generator.
    ///
    /// If not set, a random seed will be generated on each call to [`Self::make_hostile`].
    #[allow(dead_code)]
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = Some(seed);
        self
    }

    /// Make a new dataset that has a "hostile" layout.
    ///
    /// For this to be effective, there should be at least two top-level columns.
    ///
    /// By "hostile", we mean that:
    /// 1. Top-level columns are randomly split into different files. If there
    ///    are multiple fragments, they do not all have the same arrangement of
    ///    fields in data files. There is an exception for single-column data,
    ///    which will always be in a single file.
    /// 2. The field ids are not in sorted order, and have at least one hole.
    /// 3. The order of fields across the data files is random, and not
    ///    consistent across fragments.
    ///
    pub async fn make_hostile(&self, uri: &str) -> Dataset {
        let seed = self.seed.unwrap_or_else(|| rand::rng().random());
        let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
        let schema = self.make_schema(&mut rng);

        // If we only have one fragment, we should split it into two files. But
        // if we have multiple fragments, we can allow one of them to have a single
        // file. This prevents an infinite loop.
        let min_num_files = if self.data.len() > 1 { 1 } else { 2 };

        let mut fragments = Vec::with_capacity(self.data.len());
        let mut id = 0;

        for batch in &self.data {
            loop {
                let mut fragment = self
                    .make_fragment(uri, batch, &schema, &mut rng, min_num_files)
                    .await;

                let fields = field_structure(&fragment);
                let first_fields = fragments.first().map(field_structure);
                if let Some(first_fields) = first_fields {
                    if fields == first_fields && schema.fields.len() > 1 {
                        // The layout is the same as the first fragment, try again
                        // If there's only one field, then we can't expect a different
                        // layout, so there's an exception for that.
                        continue;
                    }
                }

                fragment.id = id;
                id += 1;
                fragments.push(fragment);
                break;
            }
        }

        let operation = Operation::Overwrite {
            fragments,
            schema,
            config_upsert_values: None,
            initial_bases: None,
        };

        Dataset::commit(
            uri,
            operation,
            None,
            Default::default(),
            None,
            Default::default(),
            false,
        )
        .await
        .unwrap()
    }

    fn make_schema(&self, rng: &mut impl Rng) -> Schema {
        let arrow_schema = self.data[0].schema();
        let mut schema = Schema::try_from(arrow_schema.as_ref()).unwrap();

        let field_ids = schema.fields_pre_order().map(|f| f.id).collect::<Vec<_>>();
        let mut new_ids = field_ids.clone();
        // Add a hole
        if new_ids.len() > 2 {
            let hole_pos = rng.random_range(1..new_ids.len() - 1);
            for id in new_ids.iter_mut().skip(hole_pos) {
                *id += 1;
            }
        }
        // Randomize the order of ids
        loop {
            new_ids.shuffle(rng);
            // In case we accidentally shuffled to the same order
            if new_ids.len() == 1 || new_ids != field_ids {
                break;
            }
        }
        for (old_id, new_id) in field_ids.iter().zip(new_ids.iter()) {
            let field = schema.mut_field_by_id(*old_id).unwrap();
            field.id = *new_id;
        }

        schema
    }

    async fn make_fragment(
        &self,
        uri: &str,
        batch: &RecordBatch,
        schema: &Schema,
        rng: &mut impl Rng,
        min_num_files: usize,
    ) -> Fragment {
        // Choose a random number of files.
        let num_files = if batch.num_columns() == 1 {
            1
        } else {
            rng.random_range(min_num_files..=batch.num_columns())
        };

        // Randomly assign top level fields to files.
        let column_names = batch
            .schema()
            .fields
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();
        let mut file_assignments = (0..num_files)
            .cycle()
            .take(column_names.len())
            .collect::<Vec<_>>();
        file_assignments.shuffle(rng);

        // Write each as own fragment.
        let mut sub_fragments = Vec::with_capacity(num_files);
        for file_id in 0..num_files {
            let columns = column_names
                .iter()
                .zip(file_assignments.iter())
                .filter_map(|(name, &file)| {
                    if file == file_id {
                        Some(name.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            let file_schema = schema.project(&columns).unwrap();
            let file_arrow_schema = Arc::new(ArrowSchema::from(&file_schema));
            let data = batch.project_by_schema(file_arrow_schema.as_ref()).unwrap();
            let reader = RecordBatchIterator::new(vec![Ok(data)], file_arrow_schema.clone());
            let sub_frag = FragmentCreateBuilder::new(uri)
                .schema(&file_schema)
                .write_params(&WriteParams {
                    data_storage_version: Some(self.data_storage_version),
                    ..Default::default()
                })
                .write(reader, None)
                .await
                .unwrap();

            // The sub_fragment has it's own schema, with field ids that are local to
            // it. We need to remap the field ids to the global schema.

            sub_fragments.push(sub_frag);
        }

        // Combine the fragments into a single one.
        let mut files = sub_fragments
            .into_iter()
            .flat_map(|frag| frag.files.into_iter())
            .collect::<Vec<_>>();

        // Make sure the field id order is distinct from the schema.
        let schema_field_ids = schema.fields_pre_order().map(|f| f.id).collect::<Vec<_>>();
        if files
            .iter()
            .flat_map(|file| file.fields.iter().cloned())
            .collect::<Vec<_>>()
            == schema_field_ids
            && files.len() > 1
        {
            // Swap first two files
            files.swap(0, 1);
        }

        Fragment {
            id: 0,
            files,
            deletion_file: None,
            row_id_meta: None,
            physical_rows: Some(batch.num_rows()),
            last_updated_at_version_meta: None,
            created_at_version_meta: None,
        }
    }
}

fn get_field_structure(dataset: &Dataset) -> Vec<Vec<Vec<i32>>> {
    dataset
        .get_fragments()
        .into_iter()
        .map(|frag| field_structure(frag.metadata()))
        .collect::<Vec<_>>()
}

fn field_structure(fragment: &Fragment) -> Vec<Vec<i32>> {
    fragment
        .files
        .iter()
        .map(|file| file.fields.clone())
        .collect::<Vec<_>>()
}

pub struct FragmentCount(pub u32);

impl From<u32> for FragmentCount {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

pub struct FragmentRowCount(pub u32);

impl From<u32> for FragmentRowCount {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

#[async_trait::async_trait]
pub trait DatagenExt {
    async fn into_dataset(
        self,
        path: &str,
        frag_count: FragmentCount,
        rows_per_fragment: FragmentRowCount,
    ) -> crate::Result<Dataset>
    where
        Self: Sized,
    {
        let rows_per_fragment_val = rows_per_fragment.0;
        self.into_dataset_with_params(
            path,
            frag_count,
            rows_per_fragment,
            Some(WriteParams {
                max_rows_per_file: rows_per_fragment_val as usize,
                ..Default::default()
            }),
        )
        .await
    }

    async fn into_dataset_with_params(
        self,
        path: &str,
        frag_count: FragmentCount,
        rows_per_fragment: FragmentRowCount,
        write_params: Option<WriteParams>,
    ) -> crate::Result<Dataset>;

    async fn into_ram_dataset_with_params(
        self,
        frag_count: FragmentCount,
        rows_per_fragment: FragmentRowCount,
        write_params: Option<WriteParams>,
    ) -> crate::Result<Dataset>
    where
        Self: Sized,
    {
        self.into_dataset_with_params("memory://", frag_count, rows_per_fragment, write_params)
            .await
    }

    async fn into_ram_dataset(
        self,
        frag_count: FragmentCount,
        rows_per_fragment: FragmentRowCount,
    ) -> crate::Result<Dataset>
    where
        Self: Sized,
    {
        self.into_dataset("memory://", frag_count, rows_per_fragment)
            .await
    }
}

#[async_trait::async_trait]
impl DatagenExt for BatchGeneratorBuilder {
    async fn into_dataset_with_params(
        self,
        path: &str,
        frag_count: FragmentCount,
        rows_per_fragment: FragmentRowCount,
        write_params: Option<WriteParams>,
    ) -> lance_core::Result<Dataset> {
        // Need to verify that max_rows_per_file has been set otherwise the frag_count won't be respected
        if let Some(write_params) = &write_params {
            if write_params.max_rows_per_file != rows_per_fragment.0 as usize {
                panic!(
                    "Max rows per file in write params does not match rows per fragment: {} != {}",
                    write_params.max_rows_per_file, rows_per_fragment.0 as usize
                );
            }
        } else {
            panic!("Write params are not set, will not write correct # of fragments");
        }
        let reader = self.into_reader_rows(
            RowCount::from(rows_per_fragment.0 as u64),
            BatchCount::from(frag_count.0),
        );
        Dataset::write(reader, path, write_params).await
    }
}

pub struct NoContextTestFixture {
    _tmp_dir: TempStrDir,
    pub dataset: Dataset,
}

impl Default for NoContextTestFixture {
    fn default() -> Self {
        Self::new()
    }
}

impl NoContextTestFixture {
    pub fn new() -> Self {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        runtime.block_on(async move {
            let tempdir = TempStrDir::default();
            let dataset = lance_datagen::gen_batch()
                .col(
                    "text",
                    lance_datagen::array::rand_utf8(ByteCount::from(10), false),
                )
                .into_dataset(
                    tempdir.as_str(),
                    FragmentCount::from(4),
                    FragmentRowCount::from(100),
                )
                .await
                .unwrap();
            Self {
                dataset,
                _tmp_dir: tempdir,
            }
        })
    }
}

pub fn copy_dir_all(
    src: impl AsRef<std::path::Path>,
    dst: impl AsRef<std::path::Path>,
) -> std::io::Result<()> {
    use std::fs;
    fs::create_dir_all(&dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        if ty.is_dir() {
            copy_dir_all(entry.path(), dst.as_ref().join(entry.file_name()))?;
        } else {
            fs::copy(entry.path(), dst.as_ref().join(entry.file_name()))?;
        }
    }
    Ok(())
}

/// Copies a test dataset into a temporary directory, returning the tmpdir.
///
/// The `table_path` should be relative to `test_data/` at the root of the
/// repo.
pub fn copy_test_data_to_tmp(table_path: &str) -> std::io::Result<TempDir> {
    use std::path::PathBuf;

    let mut src = PathBuf::new();
    src.push(env!("CARGO_MANIFEST_DIR"));
    src.push("../../test_data");
    src.push(table_path);

    let test_dir = TempDir::default();

    copy_dir_all(src.as_path(), test_dir.std_path())?;

    Ok(test_dir)
}

/// Result of matching a single line.
#[derive(Debug)]
enum LineMatch {
    /// Line matched (possibly via wildcards). Contains captured wildcard content.
    Matched(Vec<String>),
    /// Line is extra (in actual but not expected).
    Extra,
    /// Line is missing (in expected but not actual).
    Missing,
}

/// Result of comparing two strings.
struct MatchResult {
    lines: Vec<(String, LineMatch)>,
}

impl MatchResult {
    fn has_mismatches(&self) -> bool {
        self.lines
            .iter()
            .any(|(_, m)| matches!(m, LineMatch::Extra | LineMatch::Missing))
    }
}

/// Match a line against a pattern with `...` wildcards.
/// Returns `Some(captured_wildcards)` on match, `None` on mismatch.
fn match_line_with_wildcards(actual: &str, pattern: &str) -> Option<Vec<String>> {
    let pieces: Vec<&str> = pattern.split("...").collect();
    let mut pos = 0;
    let mut captures = Vec::new();

    for (i, piece) in pieces.iter().enumerate() {
        if i == 0 {
            // First piece must match at start
            if !actual.starts_with(piece) {
                return None;
            }
            pos = piece.len();
        } else if !piece.is_empty() {
            // Non-empty piece after a wildcard: find it in the remaining string
            let remaining = &actual[pos..];
            let found_pos = remaining.find(piece)?;
            // Capture what wildcard matched
            if found_pos > 0 {
                captures.push(remaining[..found_pos].to_string());
            }
            pos += found_pos + piece.len();
        }
        // Empty pieces (from trailing "...") are handled after the loop
    }

    // Handle trailing wildcard or check nothing remains
    if pattern.ends_with("...") {
        if pos < actual.len() {
            captures.push(actual[pos..].to_string());
        }
    } else if pos < actual.len() {
        return None;
    }

    Some(captures)
}

/// Compare actual and expected strings line by line, supporting `...` wildcards.
fn compare_strings(actual: &str, expected: &str) -> MatchResult {
    let actual_lines: Vec<&str> = actual
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .collect();
    let expected_lines: Vec<&str> = expected
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .collect();

    let mut result = MatchResult { lines: Vec::new() };
    let mut actual_idx = 0;
    let mut expected_idx = 0;

    while actual_idx < actual_lines.len() || expected_idx < expected_lines.len() {
        if actual_idx >= actual_lines.len() {
            // Remaining expected lines are missing
            result
                .lines
                .push((expected_lines[expected_idx].to_string(), LineMatch::Missing));
            expected_idx += 1;
            continue;
        }

        if expected_idx >= expected_lines.len() {
            // Remaining actual lines are extra
            result
                .lines
                .push((actual_lines[actual_idx].to_string(), LineMatch::Extra));
            actual_idx += 1;
            continue;
        }

        let actual_line = actual_lines[actual_idx];
        let expected_line = expected_lines[expected_idx];

        if let Some(captures) = match_line_with_wildcards(actual_line, expected_line) {
            // Lines match
            result
                .lines
                .push((actual_line.to_string(), LineMatch::Matched(captures)));
            actual_idx += 1;
            expected_idx += 1;
        } else {
            // Lines don't match - try to find a better alignment
            // Look ahead in actual to see if expected_line matches later
            let ahead_in_actual = actual_lines[actual_idx..]
                .iter()
                .position(|&l| match_line_with_wildcards(l, expected_line).is_some());

            // Look ahead in expected to see if actual_line matches later
            let ahead_in_expected = expected_lines[expected_idx..]
                .iter()
                .position(|&p| match_line_with_wildcards(actual_line, p).is_some());

            match (ahead_in_actual, ahead_in_expected) {
                (Some(a), Some(e)) if a <= e => {
                    // actual has extra lines before the match
                    result
                        .lines
                        .push((actual_line.to_string(), LineMatch::Extra));
                    actual_idx += 1;
                }
                (Some(_), Some(_)) => {
                    // expected has missing lines before the match
                    result
                        .lines
                        .push((expected_line.to_string(), LineMatch::Missing));
                    expected_idx += 1;
                }
                (Some(_), None) => {
                    // actual has extra lines
                    result
                        .lines
                        .push((actual_line.to_string(), LineMatch::Extra));
                    actual_idx += 1;
                }
                (None, Some(_)) => {
                    // expected has missing lines
                    result
                        .lines
                        .push((expected_line.to_string(), LineMatch::Missing));
                    expected_idx += 1;
                }
                (None, None) => {
                    // Neither matches anywhere - mark expected as missing, actual as extra
                    result
                        .lines
                        .push((expected_line.to_string(), LineMatch::Missing));
                    result
                        .lines
                        .push((actual_line.to_string(), LineMatch::Extra));
                    actual_idx += 1;
                    expected_idx += 1;
                }
            }
        }
    }

    result
}

/// Format the diff result with colors.
#[cfg(test)]
fn format_diff(result: &MatchResult) -> String {
    use yansi::Paint;

    let mut output = String::new();
    output.push_str("string mismatch\n\n");

    for (line, match_type) in &result.lines {
        match match_type {
            LineMatch::Matched(wildcards) => {
                output.push_str(&format!("  {}", line));
                if !wildcards.is_empty() {
                    let wildcard_info = format!("[... = {:?}]", wildcards);
                    output.push_str(&format!("  {}", wildcard_info.dim()));
                }
                output.push('\n');
            }
            LineMatch::Extra => {
                let extra_line = format!("+ {}", line);
                output.push_str(&format!("{}\n", extra_line.red()));
            }
            LineMatch::Missing => {
                let missing_line = format!("- {}", line);
                output.push_str(&format!("{}\n", missing_line.green()));
            }
        }
    }

    output
}

/// Implementation function for the `assert_str_matches!` macro.
/// Returns `Err(message)` if the strings don't match.
#[cfg(test)]
pub fn _assert_str_matches_impl(actual: &str, expected: &str) -> Result<(), String> {
    let result = compare_strings(actual, expected);
    if result.has_mismatches() {
        Err(format_diff(&result))
    } else {
        Ok(())
    }
}

/// Assert that actual string matches expected pattern with `...` wildcards.
///
/// Wildcards match any content within a single line:
/// - `"foo ... bar"` matches `"foo 123 bar"`
/// - `"foo ..."` matches `"foo anything"`
///
/// On failure, shows colored diff with wildcard captures.
///
/// # Example
/// ```ignore
/// assert_str_matches!("hello 123 world", "hello ... world");
/// assert_str_matches!("line1\nline2 xyz", "line1\nline2 ...");
/// ```
#[macro_export]
macro_rules! assert_str_matches {
    ($actual:expr, $expected:expr $(,)?) => {{
        let actual = $actual;
        let expected = $expected;
        if let Err(msg) = $crate::utils::test::_assert_str_matches_impl(actual, expected) {
            panic!("{}\n  at {}:{}:{}", msg, file!(), line!(), column!());
        }
    }};
}

pub async fn assert_plan_node_equals(plan_node: Arc<dyn ExecutionPlan>, raw_expected: &str) {
    let raw_plan_desc = format!(
        "{}",
        datafusion::physical_plan::displayable(plan_node.as_ref()).indent(true)
    );

    assert_str_matches!(&raw_plan_desc, raw_expected);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::dataset::WriteDestination;

    use super::*;
    use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int32Array, StringArray, StructArray};
    use arrow_schema::{DataType, Field as ArrowField, Fields as ArrowFields};
    use lance_core::utils::tempfile::TempStrDir;
    use rstest::rstest;

    #[rstest]
    #[test]
    fn test_make_schema(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("a", DataType::Int32, false),
            ArrowField::new(
                "b",
                DataType::Struct(
                    vec![
                        ArrowField::new("f1", DataType::Utf8, true),
                        ArrowField::new("f2", DataType::Boolean, false),
                    ]
                    .into(),
                ),
                true,
            ),
            ArrowField::new("c", DataType::Float64, false),
        ]));
        let data = vec![RecordBatch::new_empty(arrow_schema.clone())];

        let generator = TestDatasetGenerator::new(data, data_storage_version);
        let schema = generator.make_schema(&mut rand::rng());

        let roundtripped_schema = ArrowSchema::from(&schema);
        assert_eq!(&roundtripped_schema, arrow_schema.as_ref());

        let field_ids = schema.fields_pre_order().map(|f| f.id).collect::<Vec<_>>();
        let mut sorted_ids = field_ids.clone();
        sorted_ids.sort_unstable();
        assert_ne!(field_ids, sorted_ids);

        let mut num_holes = 0;
        for w in sorted_ids.windows(2) {
            let prev = w[0];
            let next = w[1];
            if next - prev > 1 {
                num_holes += 1;
            }
        }
        assert!(num_holes > 0, "Expected at least one hole in the field ids");
    }

    #[rstest]
    #[tokio::test]
    async fn test_make_fragment(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) {
        let tmp_dir = TempStrDir::default();

        let struct_fields: ArrowFields = vec![
            ArrowField::new("f1", DataType::Utf8, true),
            ArrowField::new("f2", DataType::Boolean, false),
        ]
        .into();
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("a", DataType::Int32, false),
            ArrowField::new("b", DataType::Struct(struct_fields.clone()), true),
            ArrowField::new("c", DataType::Float64, false),
        ]));
        let data = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StructArray::new(
                    struct_fields,
                    vec![
                        Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef,
                        Arc::new(BooleanArray::from(vec![true, false, true])),
                    ],
                    None,
                )),
                Arc::new(Float64Array::from(vec![1.1, 2.2, 3.3])),
            ],
        )
        .unwrap();

        let generator = TestDatasetGenerator::new(vec![data.clone()], data_storage_version);
        let mut rng = rand::rng();
        for _ in 1..50 {
            let schema = generator.make_schema(&mut rng);
            let fragment = generator
                .make_fragment(tmp_dir.as_str(), &data, &schema, &mut rng, 2)
                .await;

            assert!(fragment.files.len() > 1, "Expected multiple files");

            let mut field_ids_frags = fragment
                .files
                .iter()
                .flat_map(|file| file.fields.iter())
                .cloned()
                .collect::<Vec<_>>();
            let mut field_ids = schema
                .fields_pre_order()
                .filter_map(|f| {
                    if data_storage_version < LanceFileVersion::V2_1 || f.children.is_empty() {
                        Some(f.id)
                    } else {
                        // In 2.1+, struct / list fields don't have their own column
                        None
                    }
                })
                .collect::<Vec<_>>();
            field_ids_frags.sort_unstable();
            field_ids.sort_unstable();
            assert_eq!(field_ids_frags, field_ids);
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_make_hostile(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) {
        let tmp_dir = TempStrDir::default();

        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("a", DataType::Int32, false),
            ArrowField::new("b", DataType::Int32, false),
            ArrowField::new("c", DataType::Float64, false),
        ]));
        let data = vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(Int32Array::from(vec![10, 20, 30])),
                    Arc::new(Float64Array::from(vec![1.1, 2.2, 3.3])),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![4, 5, 6])),
                    Arc::new(Int32Array::from(vec![40, 50, 60])),
                    Arc::new(Float64Array::from(vec![4.4, 5.5, 6.6])),
                ],
            )
            .unwrap(),
        ];

        let seed = 42;
        let generator = TestDatasetGenerator::new(data.clone(), data_storage_version).seed(seed);

        let path = format!("{}/ds1", tmp_dir.as_str());
        let dataset = generator.make_hostile(&path).await;

        let path2 = format!("{}/ds2", tmp_dir.as_str());
        let dataset2 = generator.make_hostile(&path2).await;

        // Given the same seed, should produce the same layout.
        assert_eq!(dataset.schema(), dataset2.schema());
        let field_structure_1 = get_field_structure(&dataset);
        let field_structure_2 = get_field_structure(&dataset2);
        assert_eq!(field_structure_1, field_structure_2);

        // Make sure we handle different numbers of columns
        for num_cols in 1..4 {
            let projection = (0..num_cols).collect::<Vec<_>>();
            let data = data
                .iter()
                .map(|rb| rb.project(&projection).unwrap())
                .collect::<Vec<RecordBatch>>();

            let generator = TestDatasetGenerator::new(data.clone(), data_storage_version);
            // Sample a few
            for i in 1..20 {
                let path = format!("{}/test_ds_{}_{}", tmp_dir.as_str(), num_cols, i);
                let dataset = generator.make_hostile(&path).await;

                let field_structure = get_field_structure(&dataset);

                // The two fragments should have different layout.
                assert_eq!(field_structure.len(), 2);
                if num_cols > 1 {
                    assert_ne!(field_structure[0], field_structure[1]);
                }
            }
        }
    }

    impl<'a> From<&'a TempStrDir> for WriteDestination<'a> {
        fn from(value: &'a TempStrDir) -> Self {
            WriteDestination::Uri(value.as_str())
        }
    }

    // Tests for assert_str_matches! macro

    #[test]
    fn test_str_matches_exact() {
        crate::assert_str_matches!("hello world", "hello world");
    }

    #[test]
    fn test_str_matches_wildcard_middle() {
        crate::assert_str_matches!("foo 123 bar", "foo ... bar");
    }

    #[test]
    fn test_str_matches_wildcard_end() {
        crate::assert_str_matches!("prefix anything here", "prefix ...");
    }

    #[test]
    fn test_str_matches_wildcard_start() {
        crate::assert_str_matches!("anything prefix", "... prefix");
    }

    #[test]
    fn test_str_matches_multiple_wildcards() {
        crate::assert_str_matches!("a 1 b 2 c", "a ... b ... c");
    }

    #[test]
    fn test_str_matches_multiline() {
        crate::assert_str_matches!("line1\nline2 xyz\nline3", "line1\nline2 ...\nline3");
    }

    #[test]
    fn test_str_matches_multiline_with_whitespace() {
        crate::assert_str_matches!(
            "  line1  \n  line2 xyz  \n  line3  ",
            "line1\nline2 ...\nline3"
        );
    }

    #[test]
    fn test_str_matches_empty_wildcard() {
        // Wildcard can match empty string
        crate::assert_str_matches!("foobar", "foo...bar");
    }

    #[test]
    #[should_panic(expected = "string mismatch")]
    fn test_str_matches_mismatch_panics() {
        crate::assert_str_matches!("foo", "bar");
    }

    #[test]
    #[should_panic(expected = "string mismatch")]
    fn test_str_matches_extra_line_panics() {
        crate::assert_str_matches!("line1\nline2\nline3", "line1\nline3");
    }

    #[test]
    #[should_panic(expected = "string mismatch")]
    fn test_str_matches_missing_line_panics() {
        crate::assert_str_matches!("line1\nline3", "line1\nline2\nline3");
    }

    #[test]
    fn test_match_line_with_wildcards_basic() {
        // Exact match
        assert!(super::match_line_with_wildcards("hello world", "hello world").is_some());
        // Trailing wildcard
        assert!(super::match_line_with_wildcards("hello world", "hello ...").is_some());
        // Leading wildcard
        assert!(super::match_line_with_wildcards("hello world", "... world").is_some());
        // Just wildcard
        assert!(super::match_line_with_wildcards("hello world", "...").is_some());
        // Middle wildcard with content
        assert!(super::match_line_with_wildcards("foo 123 bar", "foo ... bar").is_some());
        // Middle wildcard with empty content (foobar matches foo...bar)
        assert!(super::match_line_with_wildcards("foobar", "foo...bar").is_some());
    }

    #[test]
    fn test_match_line_with_wildcards_captures() {
        let captures = super::match_line_with_wildcards("foo 123 bar", "foo ... bar").unwrap();
        assert_eq!(captures, vec!["123"]);

        let captures = super::match_line_with_wildcards("a 1 b 2 c", "a ... b ... c").unwrap();
        assert_eq!(captures, vec!["1", "2"]);

        let captures = super::match_line_with_wildcards("prefix suffix", "prefix ...").unwrap();
        assert_eq!(captures, vec!["suffix"]);

        // Empty capture
        let captures = super::match_line_with_wildcards("foobar", "foo...bar").unwrap();
        assert!(captures.is_empty());
    }

    #[test]
    fn test_match_line_with_wildcards_no_match() {
        assert!(super::match_line_with_wildcards("hello world", "goodbye world").is_none());
        assert!(super::match_line_with_wildcards("hello", "hello world").is_none());
        assert!(super::match_line_with_wildcards("hello world", "hello wo").is_none());
        // Pattern "hello ... world" requires something between the spaces
        // "hello world" has only one space, so it doesn't match " world" at the right position
        assert!(super::match_line_with_wildcards("hello world", "hello ... world").is_none());
    }
}
