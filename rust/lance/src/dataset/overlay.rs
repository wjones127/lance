// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Resolution of data overlay files on read.
//!
//! An overlay supplies new values for a subset of `(physical offset, field)`
//! cells. To resolve a field's values for a physical row range, the overlays
//! that cover that field are walked **newest to oldest**: the first overlay that
//! covers an offset wins, and its value is taken at the offset's **rank** (the
//! 0-based count of set bits below it) in the field's coverage bitmap. An offset
//! that no overlay covers falls through to the base value.
//!
//! Deletions take precedence over overlays, but that is handled downstream: the
//! merge runs on physical rows *before* the deletion filter, so an overlay value
//! for a deleted offset is computed and then dropped with the row — making it
//! inert, exactly as the specification requires, with no special handling here.

// This module is the tested core of overlay resolution. It is consumed by the
// scan and `take` merge paths landing in follow-up commits on this branch; until
// then its items are exercised only by unit tests.
#![allow(dead_code)]

use arrow_array::{Array, ArrayRef};
use arrow_select::interleave::interleave;
use lance_core::{Error, Result};
use roaring::RoaringBitmap;
use snafu::location;

use lance_table::format::DataOverlayFile;

/// One field's contribution from a single overlay: which physical offsets it
/// covers, and the value column holding those offsets' values (indexed by rank).
#[derive(Debug, Clone)]
pub struct ResolvedFieldOverlay {
    /// Physical offsets this overlay covers for the field.
    pub coverage: RoaringBitmap,
    /// The overlay's value column for the field. Its length must equal
    /// `coverage.len()`; the value for a covered offset `o` is at `coverage`'s
    /// rank of `o`.
    pub values: ArrayRef,
}

/// Order a fragment's overlays from newest to oldest for read resolution.
///
/// Precedence is by `committed_version` (higher is newer); ties are broken by
/// position in the fragment's `overlays` list, where a later entry is newer.
/// Returns indices into `overlays`.
pub fn overlay_indices_newest_first(overlays: &[DataOverlayFile]) -> Vec<usize> {
    let mut indices: Vec<usize> = (0..overlays.len()).collect();
    indices.sort_by(|&a, &b| {
        overlays[b]
            .committed_version
            .cmp(&overlays[a].committed_version)
            .then(b.cmp(&a))
    });
    indices
}

/// Resolve a single field's values for the physical row range
/// `[physical_start, physical_start + base.len())`, merging the overlays that
/// cover the field (which must be supplied newest-first).
///
/// The result has the same length and data type as `base`. A covered offset
/// whose overlay value is NULL resolves **to** NULL (distinct from an offset no
/// overlay covers, which keeps its base value).
pub fn resolve_overlay_column(
    base: &ArrayRef,
    physical_start: u64,
    overlays_newest_first: &[ResolvedFieldOverlay],
) -> Result<ArrayRef> {
    if overlays_newest_first.is_empty() {
        return Ok(base.clone());
    }
    for (i, overlay) in overlays_newest_first.iter().enumerate() {
        if overlay.values.len() as u64 != overlay.coverage.len() {
            return Err(Error::invalid_input(format!(
                "overlay value column {} has {} values but its coverage has {} offsets",
                i,
                overlay.values.len(),
                overlay.coverage.len()
            )));
        }
    }

    // Source 0 is the base; source k+1 is overlays_newest_first[k].values.
    let mut sources: Vec<&dyn Array> = Vec::with_capacity(overlays_newest_first.len() + 1);
    sources.push(base.as_ref());
    for overlay in overlays_newest_first {
        sources.push(overlay.values.as_ref());
    }

    let indices: Vec<(usize, usize)> = (0..base.len())
        .map(|i| {
            let offset = physical_start + i as u64;
            for (k, overlay) in overlays_newest_first.iter().enumerate() {
                if overlay.coverage.contains(offset as u32) {
                    // 0-based rank: number of set bits strictly below `offset`.
                    let rank = overlay.coverage.rank(offset as u32) as usize - 1;
                    return (k + 1, rank);
                }
            }
            (0, i)
        })
        .collect();

    interleave(&sources, &indices).map_err(|e| Error::Arrow {
        message: format!("failed to merge overlay column: {e}"),
        location: location!(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use std::sync::Arc;

    fn i32_array(values: impl IntoIterator<Item = Option<i32>>) -> ArrayRef {
        Arc::new(Int32Array::from_iter(values))
    }

    fn bitmap(offsets: impl IntoIterator<Item = u32>) -> RoaringBitmap {
        RoaringBitmap::from_iter(offsets)
    }

    fn assert_i32_eq(actual: &ArrayRef, expected: impl IntoIterator<Item = Option<i32>>) {
        let actual = actual.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(actual, &Int32Array::from_iter(expected));
    }

    #[test]
    fn test_no_overlays_returns_base() {
        let base = i32_array([Some(1), Some(2), Some(3)]);
        let resolved = resolve_overlay_column(&base, 0, &[]).unwrap();
        assert_i32_eq(&resolved, [Some(1), Some(2), Some(3)]);
    }

    #[test]
    fn test_single_overlay_rank_addressing() {
        // Base ages [30, 25, 40, 22]; overlay sets offset 1 -> 26 (rank 0).
        let base = i32_array([Some(30), Some(25), Some(40), Some(22)]);
        let overlay = ResolvedFieldOverlay {
            coverage: bitmap([1]),
            values: i32_array([Some(26)]),
        };
        let resolved = resolve_overlay_column(&base, 0, &[overlay]).unwrap();
        assert_i32_eq(&resolved, [Some(30), Some(26), Some(40), Some(22)]);
    }

    #[test]
    fn test_rank_addressing_multiple_offsets() {
        // Coverage {0, 2, 3} -> values at ranks 0,1,2.
        let base = i32_array([Some(10), Some(11), Some(12), Some(13)]);
        let overlay = ResolvedFieldOverlay {
            coverage: bitmap([0, 2, 3]),
            values: i32_array([Some(100), Some(120), Some(130)]),
        };
        let resolved = resolve_overlay_column(&base, 0, &[overlay]).unwrap();
        assert_i32_eq(&resolved, [Some(100), Some(11), Some(120), Some(130)]);
    }

    #[test]
    fn test_newest_overlay_wins() {
        // Two overlays both cover offset 1; the newest (first in the slice) wins.
        let base = i32_array([Some(0), Some(1), Some(2)]);
        let newest = ResolvedFieldOverlay {
            coverage: bitmap([1]),
            values: i32_array([Some(999)]),
        };
        let older = ResolvedFieldOverlay {
            coverage: bitmap([1, 2]),
            values: i32_array([Some(111), Some(222)]),
        };
        let resolved = resolve_overlay_column(&base, 0, &[newest, older]).unwrap();
        // offset 1 -> newest (999); offset 2 -> only older covers it (222).
        assert_i32_eq(&resolved, [Some(0), Some(999), Some(222)]);
    }

    #[test]
    fn test_null_override_vs_fall_through() {
        // A covered offset with a NULL value overrides the cell to NULL; an
        // absent offset falls through to the base.
        let base = i32_array([Some(1), Some(2), Some(3)]);
        let overlay = ResolvedFieldOverlay {
            coverage: bitmap([0]),
            values: i32_array([None]),
        };
        let resolved = resolve_overlay_column(&base, 0, &[overlay]).unwrap();
        assert_i32_eq(&resolved, [None, Some(2), Some(3)]);
    }

    #[test]
    fn test_physical_start_offset() {
        // The batch covers physical rows [10, 13); the overlay covers offset 11.
        let base = i32_array([Some(0), Some(0), Some(0)]);
        let overlay = ResolvedFieldOverlay {
            coverage: bitmap([11]),
            values: i32_array([Some(7)]),
        };
        let resolved = resolve_overlay_column(&base, 10, &[overlay]).unwrap();
        assert_i32_eq(&resolved, [Some(0), Some(7), Some(0)]);
    }

    #[test]
    fn test_string_column_merge() {
        let base: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let overlay = ResolvedFieldOverlay {
            coverage: bitmap([0, 2]),
            values: Arc::new(StringArray::from(vec!["A", "C"])),
        };
        let resolved = resolve_overlay_column(&base, 0, &[overlay]).unwrap();
        let expected: ArrayRef = Arc::new(StringArray::from(vec!["A", "b", "C"]));
        assert_eq!(&resolved, &expected);
    }

    #[test]
    fn test_value_count_mismatch_errors() {
        let base = i32_array([Some(1), Some(2)]);
        let overlay = ResolvedFieldOverlay {
            coverage: bitmap([0, 1]),
            values: i32_array([Some(9)]), // only one value for two covered offsets
        };
        assert!(resolve_overlay_column(&base, 0, &[overlay]).is_err());
    }

    #[test]
    fn test_overlay_ordering_newest_first() {
        use lance_table::format::{DataFile, OverlayCoverage};
        let mk = |version: u64| DataOverlayFile {
            data_file: DataFile::new_legacy_from_fields("o.lance", vec![1], None),
            coverage: OverlayCoverage::Shared(vec![]),
            committed_version: version,
        };
        // List order [v2, v5, v3]; newest-first should be v5(idx1), v3(idx2), v2(idx0).
        let overlays = vec![mk(2), mk(5), mk(3)];
        assert_eq!(overlay_indices_newest_first(&overlays), vec![1, 2, 0]);

        // Equal versions: later list position is newer.
        let overlays = vec![mk(4), mk(4)];
        assert_eq!(overlay_indices_newest_first(&overlays), vec![1, 0]);
    }
}
