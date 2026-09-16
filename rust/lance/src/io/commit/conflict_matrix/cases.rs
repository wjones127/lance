// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! The ordered-pair matrix, the runner that executes one cell, and the cells
//! deliberately left out of it.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use rstest::rstest;

use super::invariants;
use super::oracle;
use super::scenarios::{OVERLAY_VALUE, Scenario, Staged, fixture};
use super::{Isolation, Outcome};
use crate::Dataset;

/// What each ordered pair does today, observed rather than derived, stated
/// under [`Isolation::Legacy`].
///
/// Rows are `ours` — the transaction that has to rebase. Columns are `theirs`,
/// the one that landed underneath it, keyed by [`Scenario::code`]. Cells are
/// `L` lands, `R` retryable, `X` incompatible, and `!` excluded because what
/// the code does today is wrong (see [`KNOWN_BUGS`]).
///
/// This grid is the expectation table, not a rendering of one: it is parsed at
/// run time, so there is a single place to read and a single place to change.
/// A row landing here also opts that pair into the invariant battery, so a cell
/// is a claim about the resulting manifest as much as about the verdict.
///
/// Regenerate with the `discover` test below after a deliberate behaviour
/// change; do not hand-edit a cell to make a failing test pass. When isolation
/// levels become configurable this grows a grid per level.
const MATRIX: &str = "\
                       | ap dl ur uc ov dr pj mg ci rw ow rs
append                 | L  L  L  L  L  L  L  L  L  L  X  X
delete                 | L  R  L  R  !  R  !  R  L  R  X  X
update_rewrite_rows    | L  L  R  R  R  R  !  R  L  R  X  X
update_rewrite_columns | L  R  R  R  L  R  !  R  L  R  X  X
data_overlay           | L  L  R  L  L  L  L  R  L  R  X  X
data_replacement       | L  L  R  L  L  R  L  R  R  R  X  X
project                | L  L  L  L  L  L  R  R  L  L  X  X
merge                  | R  R  R  R  R  R  X  R  L  R  X  X
create_index           | L  L  L  L  L  R  L  L  R  R  X  X
rewrite                | L  R  R  R  R  R  L  R  R  R  X  X
overwrite              | L  L  L  L  L  L  L  L  L  L  R  L
restore                | L  L  L  L  L  L  L  L  L  L  L  L
";

/// The level [`MATRIX`] was observed under.
const MATRIX_ISOLATION: Isolation = Isolation::Legacy;

/// Parsed form of [`MATRIX`]. `None` marks a cell excluded as a known bug.
static MATRIX_CELLS: LazyLock<HashMap<(Scenario, Scenario), Option<Outcome>>> =
    LazyLock::new(parse_matrix);

fn parse_matrix() -> HashMap<(Scenario, Scenario), Option<Outcome>> {
    let scenario_by = |field: fn(Scenario) -> &'static str, value: &str| {
        Scenario::ALL
            .into_iter()
            .find(|s| field(*s) == value)
            .unwrap_or_else(|| panic!("matrix names an unknown scenario {value:?}"))
    };

    let mut lines = MATRIX.lines().filter(|line| !line.trim().is_empty());
    let header = lines.next().expect("the matrix has a header row");
    let columns: Vec<Scenario> = header
        .split_once('|')
        .expect("the header row is delimited by `|`")
        .1
        .split_whitespace()
        .map(|code| scenario_by(Scenario::code, code))
        .collect();
    assert_eq!(
        columns.len(),
        Scenario::ALL.len(),
        "the matrix header has {} columns but there are {} scenarios",
        columns.len(),
        Scenario::ALL.len(),
    );

    let mut cells = HashMap::new();
    for line in lines {
        let (label, rest) = line
            .split_once('|')
            .expect("a matrix row is delimited by `|`");
        let ours = scenario_by(Scenario::name, label.trim());
        let symbols: Vec<&str> = rest.split_whitespace().collect();
        assert_eq!(
            symbols.len(),
            columns.len(),
            "matrix row {} has {} cells but there are {} columns",
            ours.name(),
            symbols.len(),
            columns.len(),
        );
        for (theirs, symbol) in columns.iter().zip(symbols) {
            let cell = (symbol != "!").then(|| Outcome::from_symbol(symbol));
            assert!(
                cells.insert((ours, *theirs), cell).is_none(),
                "the matrix has two rows for {}",
                ours.name(),
            );
        }
    }
    cells
}

/// Render a grid in [`MATRIX`]'s format, for the `discover` tool.
fn render_matrix(symbol: impl Fn(Scenario, Scenario) -> char) -> String {
    let width = Scenario::ALL
        .into_iter()
        .map(|s| s.name().len())
        .max()
        .expect("there is at least one scenario")
        + 1;
    let mut out = format!("{:width$}| ", "");
    for theirs in Scenario::ALL {
        out.push_str(theirs.code());
        out.push(' ');
    }
    for ours in Scenario::ALL {
        out = out.trim_end().to_string();
        out.push('\n');
        out.push_str(&format!("{:width$}| ", ours.name()));
        for theirs in Scenario::ALL {
            out.push(symbol(ours, theirs));
            out.push_str("  ");
        }
    }
    out.trim_end().to_string()
}

/// Ordered pairs this suite deliberately leaves out of [`EXPECTATIONS`],
/// because what the code does with them today is wrong.
///
/// Each is written up below as a `#[ignore]`d test asserting the *correct*
/// behaviour, against the issue named here. Recording the broken behaviour as
/// an expectation instead would make the matrix assert that the bug is still
/// present, and the fix would then have to edit this table to land.
///
/// When a fix lands, remove the pair from here, un-`#[ignore]` its test, and
/// re-run `discover` to pick the cell back up.
const KNOWN_BUGS: &[(Scenario, Scenario, &str)] = &[
    (
        Scenario::Delete,
        Scenario::DataOverlay,
        "delete drops a concurrent overlay: \
         https://github.com/lance-format/lance/issues/9216",
    ),
    (
        Scenario::Delete,
        Scenario::Project,
        "delete reinstates a data file a concurrent project pruned: \
         https://github.com/lance-format/lance/issues/9217",
    ),
    (
        Scenario::UpdateRewriteRows,
        Scenario::Project,
        "update reinstates a data file a concurrent project pruned: \
         https://github.com/lance-format/lance/issues/9217",
    ),
    (
        Scenario::UpdateRewriteColumns,
        Scenario::Project,
        "update reinstates a data file a concurrent project pruned: \
         https://github.com/lance-format/lance/issues/9217",
    ),
];

fn known_bug(ours: Scenario, theirs: Scenario) -> Option<&'static str> {
    KNOWN_BUGS
        .iter()
        .find(|(o, t, _)| *o == ours && *t == theirs)
        .map(|(_, _, reason)| *reason)
}

fn expectation(ours: Scenario, theirs: Scenario) -> Option<Outcome> {
    MATRIX_CELLS.get(&(ours, theirs)).copied().flatten()
}

/// Run one ordered pair: stage `ours` against the fixture, land `theirs`
/// underneath it, then commit `ours` so it has to rebase over `theirs`.
///
/// Returns the observed outcome and, when it landed, the dataset `theirs`
/// produced alongside the one `ours` produced.
async fn run(ours: Scenario, theirs: Scenario) -> (Outcome, Option<(Arc<Dataset>, Dataset)>) {
    let base = fixture().await;

    let staged: Staged = ours
        .stage(&base)
        .await
        .unwrap_or_else(|e| panic!("staging {} failed: {e}", ours.name()));

    let concurrent = theirs
        .stage(&base)
        .await
        .unwrap_or_else(|e| panic!("staging {} failed: {e}", theirs.name()));
    let after_theirs = Arc::new(
        concurrent
            .commit(&base)
            .await
            .unwrap_or_else(|e| panic!("{} could not land uncontended: {e}", theirs.name())),
    );

    let result = staged.commit(&base).await;
    let outcome = Outcome::of(&result);
    let landed = result.ok().map(|dataset| (after_theirs, dataset));
    (outcome, landed)
}

/// Every ordered pair in one row of the matrix, plus the invariant battery on
/// each landing.
///
/// One test per row rather than per cell: a row is twelve commits against a
/// six-row in-memory dataset, which stays well inside the per-test budget while
/// keeping a failure pinned to a named row.
#[rstest]
#[case::append(Scenario::Append)]
#[case::delete(Scenario::Delete)]
#[case::update_rewrite_rows(Scenario::UpdateRewriteRows)]
#[case::update_rewrite_columns(Scenario::UpdateRewriteColumns)]
#[case::data_overlay(Scenario::DataOverlay)]
#[case::data_replacement(Scenario::DataReplacement)]
#[case::project(Scenario::Project)]
#[case::merge(Scenario::Merge)]
#[case::create_index(Scenario::CreateIndex)]
#[case::rewrite(Scenario::Rewrite)]
#[case::overwrite(Scenario::Overwrite)]
#[case::restore(Scenario::Restore)]
#[tokio::test]
async fn matrix_row(#[case] ours: Scenario) {
    for theirs in Scenario::ALL {
        if known_bug(ours, theirs).is_some() {
            continue;
        }
        let expected = expectation(ours, theirs).unwrap_or_else(|| {
            panic!(
                "({}, {}) is neither in EXPECTATIONS nor in KNOWN_BUGS; re-run `discover`",
                ours.name(),
                theirs.name(),
            )
        });
        let context = format!("({} over {})", ours.name(), theirs.name());
        let (outcome, landed) = run(ours, theirs).await;
        assert_eq!(
            outcome, expected,
            "{context}: expected {expected:?} but observed {outcome:?}",
        );
        if let Some((before, after)) = landed {
            invariants::check_all(&before, &after, ours, &context).await;
            // Boxed: the oracle runs a whole second commit sequence, so its
            // future is large enough to trip `clippy::large_futures` inline.
            Box::pin(oracle::check(
                MATRIX_ISOLATION,
                ours,
                theirs,
                &after,
                &context,
            ))
            .await;
        }
    }
}

/// Every scenario must land and read back correctly with nothing to rebase
/// over.
///
/// This is the harness's own precondition: a matrix cell that fails is only
/// evidence about conflict resolution if the operation is sound on its own.
#[rstest]
#[case::append(Scenario::Append)]
#[case::delete(Scenario::Delete)]
#[case::update_rewrite_rows(Scenario::UpdateRewriteRows)]
#[case::update_rewrite_columns(Scenario::UpdateRewriteColumns)]
#[case::data_overlay(Scenario::DataOverlay)]
#[case::data_replacement(Scenario::DataReplacement)]
#[case::project(Scenario::Project)]
#[case::merge(Scenario::Merge)]
#[case::create_index(Scenario::CreateIndex)]
#[case::rewrite(Scenario::Rewrite)]
#[case::overwrite(Scenario::Overwrite)]
#[case::restore(Scenario::Restore)]
#[tokio::test]
async fn scenario_lands_uncontended(#[case] scenario: Scenario) {
    let base = fixture().await;
    let staged = scenario
        .stage(&base)
        .await
        .unwrap_or_else(|e| panic!("staging {} failed: {e}", scenario.name()));
    let committed = staged
        .commit(&base)
        .await
        .unwrap_or_else(|e| panic!("{} could not land uncontended: {e}", scenario.name()));
    let context = format!("({} uncontended)", scenario.name());
    invariants::check_all(&base, &committed, scenario, &context).await;
}

/// The matrix must cover the whole grid, and its exclusions must be exactly the
/// pairs [`KNOWN_BUGS`] documents.
///
/// The grid and the bug list are separate on purpose — the grid says which cells
/// are excluded, the list says why and against which issue — so they have to be
/// held to agreeing.
#[test]
fn matrix_is_total() {
    assert_eq!(
        MATRIX_CELLS.len(),
        Scenario::ALL.len() * Scenario::ALL.len(),
        "the matrix does not cover every ordered pair",
    );
    for ours in Scenario::ALL {
        for theirs in Scenario::ALL {
            let cell = MATRIX_CELLS.get(&(ours, theirs)).unwrap_or_else(|| {
                panic!(
                    "({}, {}) is missing from the matrix; re-run `discover`",
                    ours.name(),
                    theirs.name()
                )
            });
            let bug = known_bug(ours, theirs);
            assert_eq!(
                cell.is_none(),
                bug.is_some(),
                "({}, {}) is marked {} in the matrix but {} in KNOWN_BUGS",
                ours.name(),
                theirs.name(),
                if cell.is_none() {
                    "excluded"
                } else {
                    "expected"
                },
                if bug.is_some() { "listed" } else { "absent" },
            );
        }
    }
}

/// A `Delete` must not drop an overlay that landed concurrently.
///
/// `Operation::Delete`'s apply replaces the fragment entry wholesale from a
/// post-image built at the read version, and — unlike `Operation::Update`'s arm
/// — does not carry `overlays` forward. `check_delete_txn` explicitly permits a
/// concurrent `DataOverlay`, so the pair is allowed to land and the overlay is
/// silently lost. Ordering-dependent: overlay-after-delete is fine.
///
/// The loss is observable from a plain scan, not just from the manifest: the
/// overlaid cell reverts to its base value.
#[tokio::test]
#[ignore = "bug: https://github.com/lance-format/lance/issues/9216"]
async fn delete_must_not_drop_a_concurrent_overlay() {
    let base = fixture().await;
    let staged = Scenario::Delete.stage(&base).await.unwrap();

    let overlay = Scenario::DataOverlay.stage(&base).await.unwrap();
    let after_overlay = overlay.commit(&base).await.unwrap();
    let overlaid = after_overlay.scan().try_into_batch().await.unwrap();
    assert!(
        int_column(&overlaid, "b").contains(&OVERLAY_VALUE),
        "precondition: the overlay is visible before the delete",
    );

    let committed = staged.commit(&base).await.unwrap();
    let after = committed.scan().try_into_batch().await.unwrap();
    assert!(
        int_column(&after, "b").contains(&OVERLAY_VALUE),
        "the delete dropped the concurrently committed overlay: `b` reads back as {:?}, \
         with the overlaid value {OVERLAY_VALUE} gone",
        int_column(&after, "b"),
    );
}

/// A `Delete` must not reinstate a data file a concurrent `Project` pruned.
///
/// Same mechanism as the overlay bug: the post-image was built before the
/// `Project` landed, so a file the projection dropped whole comes back. The
/// resulting manifest has a data file none of whose fields are in the schema,
/// which also pushes `Manifest::max_field_id` back up over a field id the
/// projection had retired — so a later write can mint an id that is already in
/// a data file.
#[tokio::test]
#[ignore = "bug: https://github.com/lance-format/lance/issues/9217"]
async fn delete_must_not_reinstate_a_pruned_data_file() {
    let base = fixture().await;
    let staged = Scenario::Delete.stage(&base).await.unwrap();

    let project = Scenario::Project.stage(&base).await.unwrap();
    let after_project = project.commit(&base).await.unwrap();
    assert_eq!(
        after_project.fragments()[0].files.len(),
        1,
        "precondition: the projection pruned `c`'s data file",
    );
    let watermark = after_project.manifest.max_field_id();

    let committed = staged.commit(&base).await.unwrap();
    assert_eq!(
        committed.fragments()[0].files.len(),
        1,
        "the delete reinstated the data file the projection pruned: fragment 0 carries {:?}",
        committed.fragments()[0]
            .files
            .iter()
            .map(|f| f.fields.clone())
            .collect::<Vec<_>>(),
    );
    assert!(
        committed.manifest.max_field_id() <= watermark,
        "the field-id watermark went back up from {watermark} to {} across the delete",
        committed.manifest.max_field_id(),
    );
}

/// An `Update` must not reinstate a data file a concurrent `Project` pruned.
///
/// The same defect as [`delete_must_not_reinstate_a_pruned_data_file`], reached
/// through `Operation::Update`'s apply instead of `Operation::Delete`'s: both
/// install a post-image of the fragment built at the read version. Recorded
/// separately because fixing one arm does not fix the other.
#[tokio::test]
#[ignore = "bug: https://github.com/lance-format/lance/issues/9217"]
async fn update_must_not_reinstate_a_pruned_data_file() {
    let base = fixture().await;
    let staged = Scenario::UpdateRewriteRows.stage(&base).await.unwrap();

    let project = Scenario::Project.stage(&base).await.unwrap();
    let after_project = project.commit(&base).await.unwrap();
    assert_eq!(
        after_project.fragments()[0].files.len(),
        1,
        "precondition: the projection pruned `c`'s data file",
    );

    let committed = staged.commit(&base).await.unwrap();
    let live: Vec<i32> = committed
        .manifest
        .schema
        .fields_pre_order()
        .map(|field| field.id)
        .collect();
    for fragment in committed.fragments().iter() {
        for file in fragment.files.iter() {
            assert!(
                file.fields.iter().any(|id| live.contains(id)),
                "the update reinstated a data file the projection pruned: fragment {} carries \
                 fields {:?}, none of which are in the schema {live:?}",
                fragment.id,
                file.fields,
            );
        }
    }
}

fn int_column(batch: &arrow_array::RecordBatch, name: &str) -> Vec<i32> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int32Type;
    batch[name]
        .as_primitive::<Int32Type>()
        .iter()
        .flatten()
        .collect()
}

/// Print the observed outcome for every ordered pair, as a grid.
///
/// Not an assertion — this is the tool that regenerates [`MATRIX`]. Run it with:
///
/// ```text
/// cargo test -p lance --lib conflict_matrix::cases::discover -- --ignored --nocapture
/// ```
///
/// Cells that are excluded in the current matrix are re-emitted as `!` rather
/// than as whatever they do, so regenerating never silently turns a known bug
/// back into an expectation.
#[tokio::test]
#[ignore = "discovery tool, not an assertion; regenerates the MATRIX grid"]
// Stdout is the output format here: this test's job is to emit a grid to paste
// back into `MATRIX`, so a logging framework would be the wrong sink.
#[allow(clippy::print_stdout)]
async fn discover() {
    let mut observed = HashMap::new();
    for ours in Scenario::ALL {
        for theirs in Scenario::ALL {
            let symbol = if known_bug(ours, theirs).is_some() {
                '!'
            } else {
                run(ours, theirs).await.0.symbol()
            };
            observed.insert((ours, theirs), symbol);
        }
    }
    println!(
        "{}",
        render_matrix(|ours, theirs| observed[&(ours, theirs)])
    );
}
