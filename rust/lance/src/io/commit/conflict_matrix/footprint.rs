// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Whether a pair conflicts because of *what it touches*, or only because of
//! *what it is*.
//!
//! The grid in [`super::cases`] stages every operation against fragment 0, so
//! every fragment-scoped pair in it overlaps. That makes it a good detector of
//! under-rejection — a pair that should have conflicted and did not — and a poor
//! one for the opposite mistake. A conflict detector that rejected every pair
//! outright would score nearly as well on it as one that reasons about
//! footprints, because the matrix never asks a question whose answer depends on
//! the footprint.
//!
//! This module asks it. `ours` still works on fragment 0; `theirs` is staged at
//! [`Footprint::Same`] and again at [`Footprint::Disjoint`], which is fragment 1.
//! A cell where the two differ is a pair legacy resolves by footprint. A cell
//! where they agree is one it resolves by opcode alone — which may be correct,
//! or may be conservatism worth revisiting, but either way it is now recorded
//! rather than unasked.
//!
//! Only the fragment-scoped operations appear. The rest produce the same
//! transaction whichever fragment they are pointed at, so a second column would
//! cost runtime and assert nothing — see [`Scenario::is_fragment_scoped`].
//!
//! # Field-level overlap is not covered
//!
//! Two operations can touch the same fragment and still be disjoint, by
//! touching different columns of it. That distinction only means anything for
//! the three field-scoped operations, and the fixture cannot express it cleanly:
//! `a` and `b` share a data file, so "a different field" is only available in
//! one direction. It is a follow-up, not a thing this module quietly approximates.

use std::collections::HashMap;
use std::sync::LazyLock;

use rstest::rstest;

use super::cases::{known_bug, run};
use super::scenarios::{Footprint, Scenario};
use super::{Isolation, Outcome};
use super::{invariants, oracle};

/// What each fragment-scoped ordered pair does at each footprint, stated under
/// [`Isolation::Legacy`].
///
/// Rows are `ours`, columns are `theirs`, keyed by [`Scenario::code`]. Each cell
/// is `same/disjoint`: what happens when `theirs` touches the same fragment as
/// `ours`, and when it touches a different one. `L` lands, `R` retryable, `X`
/// incompatible, `!` excluded as a known bug.
///
/// A cell reading `R/L` is footprint-sensitive: legacy rejects the overlap and
/// permits the disjoint case. A cell reading `R/R` rejects on opcode alone.
///
/// Regenerate with the `discover_footprints` test below.
const FOOTPRINT_MATRIX: &str = "\
                       | dl    df    ur    uc    ov    dr    ci    rw
delete                 | R/L   R/L   L/L   R/L   !/!   R/L   L/L   R/L
delete_whole_fragment  | R/L   R/L   R/L   R/L   L/L   R/L   L/L   R/L
update_rewrite_rows    | L/L   R/L   R/L   R/L   R/L   R/L   L/L   R/L
update_rewrite_columns | R/L   R/L   R/L   R/L   L/L   R/L   L/L   R/L
data_overlay           | L/L   R/L   R/L   L/L   L/L   L/L   L/L   R/L
data_replacement       | L/L   X/L   R/L   L/L   L/L   R/L   R/R   R/L
create_index           | L/L   L/L   L/L   L/L   L/L   R/R   R/R   R/L
rewrite                | R/L   R/L   R/L   R/L   R/L   R/L   R/L   R/L
";

/// The level [`FOOTPRINT_MATRIX`] was observed under.
const FOOTPRINT_ISOLATION: Isolation = Isolation::Legacy;

/// Parsed form of [`FOOTPRINT_MATRIX`], keyed by pair then footprint. `None`
/// marks a cell excluded as a known bug.
static FOOTPRINT_CELLS: LazyLock<HashMap<(Scenario, Scenario, Footprint), Option<Outcome>>> =
    LazyLock::new(parse);

/// The operations this module covers, in matrix order.
fn scoped() -> Vec<Scenario> {
    Scenario::ALL
        .into_iter()
        .filter(|scenario| scenario.is_fragment_scoped())
        .collect()
}

fn parse() -> HashMap<(Scenario, Scenario, Footprint), Option<Outcome>> {
    let scenario_by = |field: fn(Scenario) -> &'static str, value: &str| {
        Scenario::ALL
            .into_iter()
            .find(|s| field(*s) == value)
            .unwrap_or_else(|| panic!("the footprint matrix names an unknown scenario {value:?}"))
    };

    let mut lines = FOOTPRINT_MATRIX
        .lines()
        .filter(|line| !line.trim().is_empty());
    let header = lines.next().expect("the footprint matrix has a header row");
    let columns: Vec<Scenario> = header
        .split_once('|')
        .expect("the header row is delimited by `|`")
        .1
        .split_whitespace()
        .map(|code| scenario_by(Scenario::code, code))
        .collect();
    assert_eq!(
        columns,
        scoped(),
        "the footprint matrix columns must be the fragment-scoped scenarios, in order",
    );

    let mut cells = HashMap::new();
    for line in lines {
        let (label, rest) = line
            .split_once('|')
            .expect("a footprint matrix row is delimited by `|`");
        let ours = scenario_by(Scenario::name, label.trim());
        let entries: Vec<&str> = rest.split_whitespace().collect();
        assert_eq!(
            entries.len(),
            columns.len(),
            "footprint matrix row {} has {} cells but there are {} columns",
            ours.name(),
            entries.len(),
            columns.len(),
        );
        for (theirs, entry) in columns.iter().zip(entries) {
            let (same, disjoint) = entry
                .split_once('/')
                .unwrap_or_else(|| panic!("cell {entry:?} is not in `same/disjoint` form"));
            for (footprint, symbol) in [(Footprint::Same, same), (Footprint::Disjoint, disjoint)] {
                let cell = (symbol != "!").then(|| Outcome::from_symbol(symbol));
                assert!(
                    cells.insert((ours, *theirs, footprint), cell).is_none(),
                    "the footprint matrix has two rows for {}",
                    ours.name(),
                );
            }
        }
    }
    cells
}

/// One row of the footprint matrix: `ours` against every fragment-scoped
/// `theirs`, at both footprints.
#[rstest]
#[case::delete(Scenario::Delete)]
#[case::delete_whole_fragment(Scenario::DeleteWholeFragment)]
#[case::update_rewrite_rows(Scenario::UpdateRewriteRows)]
#[case::update_rewrite_columns(Scenario::UpdateRewriteColumns)]
#[case::data_overlay(Scenario::DataOverlay)]
#[case::data_replacement(Scenario::DataReplacement)]
#[case::create_index(Scenario::CreateIndex)]
#[case::rewrite(Scenario::Rewrite)]
#[tokio::test]
async fn footprint_row(#[case] ours: Scenario) {
    for theirs in scoped() {
        for footprint in Footprint::ALL {
            let Some(expected) = FOOTPRINT_CELLS
                .get(&(ours, theirs, footprint))
                .unwrap_or_else(|| {
                    panic!(
                        "({}, {}) is missing from the footprint matrix; re-run `discover_footprints`",
                        ours.name(),
                        theirs.name(),
                    )
                })
            else {
                continue;
            };
            let context = format!(
                "({} over {} touching a {} fragment)",
                ours.name(),
                theirs.name(),
                footprint.name(),
            );
            let (outcome, landed) = run(ours, theirs, footprint).await;
            assert_eq!(
                outcome, *expected,
                "{context}: expected {expected:?} but observed {outcome:?}",
            );
            if let Some((before, after)) = landed {
                invariants::check_all(&before, &after, ours, &context).await;
                // Boxed for the same reason as in `cases`: a second commit
                // sequence makes the future large enough to trip clippy.
                Box::pin(oracle::check(
                    FOOTPRINT_ISOLATION,
                    ours,
                    theirs,
                    footprint,
                    &after,
                    &context,
                ))
                .await;
            }
        }
    }
}

/// Every pair must be covered at both footprints, and a pair excluded at one
/// footprint must be excluded at both.
///
/// The second half is the interesting one: `KNOWN_BUGS` is keyed on the pair,
/// not on the footprint, so a bug that only reproduces when the footprints
/// overlap would be silently exempted here too. Holding the two footprints to
/// the same exclusion makes that visible.
#[test]
fn footprint_matrix_is_total() {
    for ours in scoped() {
        for theirs in scoped() {
            let cells: Vec<_> = Footprint::ALL
                .into_iter()
                .map(|footprint| {
                    *FOOTPRINT_CELLS
                        .get(&(ours, theirs, footprint))
                        .unwrap_or_else(|| {
                            panic!(
                                "({}, {}) at {} is missing from the footprint matrix",
                                ours.name(),
                                theirs.name(),
                                footprint.name(),
                            )
                        })
                })
                .collect();
            let excluded = cells.iter().filter(|cell| cell.is_none()).count();
            assert!(
                excluded == 0 || excluded == cells.len(),
                "({}, {}) is excluded at some footprints but not others",
                ours.name(),
                theirs.name(),
            );
            assert_eq!(
                excluded > 0,
                known_bug(ours, theirs).is_some(),
                "({}, {}) disagrees with KNOWN_BUGS about being excluded",
                ours.name(),
                theirs.name(),
            );
        }
    }
}

/// Print the observed outcome for every fragment-scoped pair at both
/// footprints, as a grid.
///
/// Not an assertion — this regenerates [`FOOTPRINT_MATRIX`]. Run it with:
///
/// ```text
/// cargo test -p lance --lib conflict_matrix::footprint::discover_footprints -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore = "discovery tool, not an assertion; regenerates the FOOTPRINT_MATRIX grid"]
// Stdout is the output format here, as in `cases::discover`.
#[allow(clippy::print_stdout)]
async fn discover_footprints() {
    let scoped = scoped();
    let width = scoped
        .iter()
        .map(|s| s.name().len())
        .max()
        .expect("there is at least one fragment-scoped scenario")
        + 1;

    let mut out = format!("{:width$}| ", "");
    for theirs in &scoped {
        out.push_str(&format!("{:<6}", theirs.code()));
    }
    for ours in &scoped {
        out = out.trim_end().to_string();
        out.push('\n');
        out.push_str(&format!("{:width$}| ", ours.name()));
        for theirs in &scoped {
            let mut entry = String::new();
            for footprint in Footprint::ALL {
                if !entry.is_empty() {
                    entry.push('/');
                }
                if known_bug(*ours, *theirs).is_some() {
                    entry.push('!');
                } else {
                    entry.push(run(*ours, *theirs, footprint).await.0.symbol());
                }
            }
            out.push_str(&format!("{entry:<6}"));
        }
    }
    println!("{}", out.trim_end());
}
