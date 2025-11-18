// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use deepsize::DeepSizeOf;

use super::{RowAddrTreeMap, RowIdMask};

/// A set of row ids, with optional set of nulls.
///
/// This is often a result of a filter, where `selected` represents the rows that
/// passed the filter, and `nulls` represents the rows where the filter evaluated
/// to null. For example, in SQL `NULL > 5` evaluates to null. This is distinct
/// from being deselected to support proper three-valued logic for NOT.
/// (`NOT FALSE` is TRUE, `NOT TRUE` is FALSE, but `NOT NULL` is NULL.
/// `NULL | TRUE = TRUE`, `NULL & FALSE = FALSE`, but `NULL | FALSE = NULL`
/// and `NULL & TRUE = NULL`).
#[derive(Clone, Debug, Default, DeepSizeOf)]
pub struct NullableRowAddrSet {
    selected: RowAddrTreeMap,
    // Rows that are NULL. These rows are considered NULL even if they are also in `selected`.
    nulls: RowAddrTreeMap,
}

impl NullableRowAddrSet {
    /// Create a new RowSelection from selected rows and null rows.
    ///
    /// `nulls` may have overlap with `selected`. Rows in `nulls` are considered NULL,
    /// even if they are also in `selected`.
    pub fn new(selected: RowAddrTreeMap, nulls: RowAddrTreeMap) -> Self {
        Self { selected, nulls }
    }

    pub fn with_nulls(mut self, nulls: RowAddrTreeMap) -> Self {
        self.nulls = nulls;
        self
    }

    /// Create an empty selection. Alias for [Default::default]
    pub fn empty() -> Self {
        Default::default()
    }

    /// Get the number of TRUE rows (selected but not null).
    ///
    /// Returns None if the number of TRUE rows cannot be determined. This happens
    /// if the underlying RowAddrTreeMap has full fragments selected.
    pub fn len(&self) -> Option<u64> {
        self.true_rows().len()
    }

    pub fn is_empty(&self) -> bool {
        self.selected.is_empty()
    }

    /// Check if a row_id is selected (TRUE)
    pub fn selected(&self, row_id: u64) -> bool {
        self.selected.contains(row_id) && !self.nulls.contains(row_id)
    }

    /// Get the null rows
    pub fn null_rows(&self) -> &RowAddrTreeMap {
        &self.nulls
    }

    /// Get the TRUE rows (selected but not null)
    pub fn true_rows(&self) -> RowAddrTreeMap {
        self.selected.clone() - self.nulls.clone()
    }

    pub fn union_all(selections: &[Self]) -> Self {
        let true_rows = selections
            .iter()
            .map(|s| s.true_rows())
            .collect::<Vec<RowAddrTreeMap>>();
        let true_rows_refs = true_rows.iter().collect::<Vec<&RowAddrTreeMap>>();
        let selected = RowAddrTreeMap::union_all(&true_rows_refs);
        let nulls = RowAddrTreeMap::union_all(
            &selections
                .iter()
                .map(|s| &s.nulls)
                .collect::<Vec<&RowAddrTreeMap>>(),
        );
        // TRUE | NULL = TRUE, so remove any TRUE rows from nulls
        let nulls = nulls - &selected;
        Self { selected, nulls }
    }
}

impl PartialEq for NullableRowAddrSet {
    fn eq(&self, other: &Self) -> bool {
        self.true_rows() == other.true_rows() && self.nulls == other.nulls
    }
}

impl std::ops::BitAndAssign<&Self> for NullableRowAddrSet {
    fn bitand_assign(&mut self, rhs: &Self) {
        self.nulls = if self.nulls.is_empty() && rhs.nulls.is_empty() {
            RowAddrTreeMap::new() // Fast path
        } else {
            (self.nulls.clone() & &rhs.nulls) // null and null -> null
            | (self.nulls.clone() & &rhs.selected) // null and true -> null
            | (rhs.nulls.clone() & &self.selected) // true and null -> null
        };

        self.selected &= &rhs.selected;
    }
}

impl std::ops::BitOrAssign<&Self> for NullableRowAddrSet {
    fn bitor_assign(&mut self, rhs: &Self) {
        self.nulls = if self.nulls.is_empty() && rhs.nulls.is_empty() {
            RowAddrTreeMap::new() // Fast path
        } else {
            // null or null -> null (excluding rows that are true in either)
            let true_rows =
                (self.selected.clone() - &self.nulls) | (rhs.selected.clone() - &rhs.nulls);
            (self.nulls.clone() | &rhs.nulls) - true_rows
        };

        self.selected |= &rhs.selected;
    }
}

/// A version of [`RowIdMask`] that supports nulls.
///
/// This mask handles three-valued logic for SQL expressions, where a filter can
/// evaluate to TRUE, FALSE, or NULL. The `selected` set includes rows that are
/// TRUE or NULL. The `nulls` set includes rows that are NULL.
#[derive(Clone, Debug)]
pub enum NullableRowIdMask {
    AllowList(NullableRowAddrSet),
    BlockList(NullableRowAddrSet),
}

impl NullableRowIdMask {
    pub fn selected(&self, row_id: u64) -> bool {
        match self {
            Self::AllowList(NullableRowAddrSet { selected, nulls }) => {
                selected.contains(row_id) && !nulls.contains(row_id)
            }
            Self::BlockList(NullableRowAddrSet { selected, nulls }) => {
                !selected.contains(row_id) && !nulls.contains(row_id)
            }
        }
    }

    pub fn drop_nulls(self) -> RowIdMask {
        match self {
            Self::AllowList(NullableRowAddrSet { selected, nulls }) => {
                RowIdMask::AllowList(selected - nulls)
            }
            Self::BlockList(NullableRowAddrSet { selected, nulls }) => {
                RowIdMask::BlockList(selected | nulls)
            }
        }
    }
}

impl std::ops::Not for NullableRowIdMask {
    type Output = Self;

    fn not(self) -> Self::Output {
        match self {
            Self::AllowList(set) => Self::BlockList(set),
            Self::BlockList(set) => Self::AllowList(set),
        }
    }
}

impl std::ops::BitAnd for NullableRowIdMask {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        // Null handling:
        // * null and true -> null
        // * null and null -> null
        // * null and false -> false
        match (self, rhs) {
            (Self::AllowList(a), Self::AllowList(b)) => {
                let nulls = if a.nulls.is_empty() && b.nulls.is_empty() {
                    RowAddrTreeMap::new() // Fast path
                } else {
                    (a.nulls.clone() & &b.nulls) // null and null -> null
                    | (a.nulls & &b.selected) // null and true -> null
                    | (b.nulls & &a.selected) // true and null -> null
                };
                let selected = a.selected & b.selected;
                Self::AllowList(NullableRowAddrSet { selected, nulls })
            }
            (Self::AllowList(allow), Self::BlockList(block))
            | (Self::BlockList(block), Self::AllowList(allow)) => {
                let nulls = if allow.nulls.is_empty() && block.nulls.is_empty() {
                    RowAddrTreeMap::new() // Fast path
                } else {
                    (allow.nulls.clone() & &block.nulls) // null and null -> null
                    | (allow.nulls - &block.selected) // null and true -> null
                    | (block.nulls & &allow.selected) // true and null -> null
                };
                let selected = allow.selected - block.selected;
                Self::AllowList(NullableRowAddrSet { selected, nulls })
            }
            (Self::BlockList(a), Self::BlockList(b)) => {
                let nulls = if a.nulls.is_empty() && b.nulls.is_empty() {
                    RowAddrTreeMap::new() // Fast path
                } else {
                    (a.nulls.clone() & &b.nulls) // null and null -> null
                    | (a.nulls - &b.selected) // null and true -> null
                    | (b.nulls - &a.selected) // true and null -> null
                };
                let selected = a.selected | b.selected;
                Self::BlockList(NullableRowAddrSet { selected, nulls })
            }
        }
    }
}

impl std::ops::BitOr for NullableRowIdMask {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        // Null handling:
        // * null or true -> true
        // * null or null -> null
        // * null or false -> null
        match (self, rhs) {
            (Self::AllowList(a), Self::AllowList(b)) => {
                let nulls = if a.nulls.is_empty() && b.nulls.is_empty() {
                    RowAddrTreeMap::new() // Fast path
                } else {
                    // null or null -> null (excluding rows that are true in either)
                    let true_rows =
                        (a.selected.clone() - &a.nulls) | (b.selected.clone() - &b.nulls);
                    (a.nulls | b.nulls) - true_rows
                };
                let selected = (a.selected | b.selected) | &nulls;
                Self::AllowList(NullableRowAddrSet { selected, nulls })
            }
            (Self::AllowList(allow), Self::BlockList(block))
            | (Self::BlockList(block), Self::AllowList(allow)) => {
                let nulls = if allow.nulls.is_empty() && block.nulls.is_empty() {
                    RowAddrTreeMap::new() // Fast path
                } else {
                    // null or null -> null (excluding rows that are true in either)
                    let allow_true = allow.selected.clone() - &allow.nulls;
                    ((allow.nulls | block.nulls) & block.selected.clone()) - allow_true
                };
                let selected = (block.selected - allow.selected) | &nulls;
                Self::BlockList(NullableRowAddrSet { selected, nulls })
            }
            (Self::BlockList(a), Self::BlockList(b)) => {
                let nulls = if a.nulls.is_empty() && b.nulls.is_empty() {
                    RowAddrTreeMap::new() // Fast path
                } else {
                    // null or null -> null (excluding rows that are true in either)
                    let false_rows =
                        (a.selected.clone() - &a.nulls) & (b.selected.clone() - &b.nulls);
                    (a.nulls | &b.nulls) - false_rows
                };
                let selected = (a.selected & b.selected) | &nulls;
                Self::BlockList(NullableRowAddrSet { selected, nulls })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_not_with_nulls() {
        // Test case from issue #4756: x != 5 on data [0, 5, null]
        // x = 5 should return: AllowList with selected=[1,2], nulls=[2]
        // NOT(x = 5) should return: BlockList with selected=[1,2], nulls=[2]
        // selected() should return TRUE for row 0, FALSE for rows 1 and 2
        let mask = NullableRowIdMask::AllowList(NullableRowAddrSet::new(
            RowAddrTreeMap::from_iter(&[1, 2]), // rows where x==5 or x==null
            RowAddrTreeMap::from_iter(&[2]),    // row where x is null
        ));

        let not_mask = !mask;

        // Row 0: should be selected (x=0, which is != 5)
        assert!(
            not_mask.selected(0),
            "Row 0 (x=0) should be selected for x != 5"
        );

        // Row 1: should NOT be selected (x=5, which is == 5)
        assert!(
            !not_mask.selected(1),
            "Row 1 (x=5) should NOT be selected for x != 5"
        );

        // Row 2: should NOT be selected (x=null, comparison result is null)
        assert!(
            !not_mask.selected(2),
            "Row 2 (x=null) should NOT be selected for x != 5"
        );
    }

    #[test]
    fn test_and_with_nulls() {
        // Test Kleene AND logic: true AND null = null, false AND null = false

        // Case 1: TRUE mask AND mask with nulls
        let true_mask = NullableRowIdMask::AllowList(NullableRowAddrSet::new(
            RowAddrTreeMap::from_iter(&[0, 1, 2, 3, 4]), // All TRUE
            RowAddrTreeMap::new(),                       // No nulls
        ));
        let null_mask = NullableRowIdMask::AllowList(NullableRowAddrSet::new(
            RowAddrTreeMap::from_iter(&[0, 1, 2, 3, 4]), // TRUE or NULL
            RowAddrTreeMap::from_iter(&[1, 3]),          // NULL rows
        ));
        let result = true_mask & null_mask.clone();

        // TRUE AND TRUE = TRUE
        assert!(result.selected(0));
        assert!(result.selected(2));
        assert!(result.selected(4));
        // TRUE AND NULL = NULL (filtered out)
        assert!(!result.selected(1));
        assert!(!result.selected(3));

        // Case 2: FALSE mask AND mask with nulls
        let false_mask = NullableRowIdMask::BlockList(NullableRowAddrSet::new(
            RowAddrTreeMap::from_iter(&[0, 1, 2, 3, 4]), // All FALSE
            RowAddrTreeMap::new(),                       // No nulls
        ));
        let result = false_mask & null_mask;

        // FALSE AND anything = FALSE
        assert!(!result.selected(0));
        assert!(!result.selected(1));
        assert!(!result.selected(2));
        assert!(!result.selected(3));
        assert!(!result.selected(4));

        // Case 3: Both masks have nulls - union of null sets
        let mask1 = NullableRowIdMask::AllowList(NullableRowAddrSet::new(
            RowAddrTreeMap::from_iter(&[0, 1, 2]), // TRUE or NULL
            RowAddrTreeMap::from_iter(&[1]),       // NULL rows
        ));
        let mask2 = NullableRowIdMask::AllowList(NullableRowAddrSet::new(
            RowAddrTreeMap::from_iter(&[0, 2, 3]), // TRUE or NULL
            RowAddrTreeMap::from_iter(&[2]),       // NULL rows
        ));
        let result = mask1 & mask2;

        // Only row 0 is TRUE in both
        assert!(result.selected(0));
        // Rows 1, 2 are null in at least one
        assert!(!result.selected(1));
        assert!(!result.selected(2));
        // Row 3 is not in first mask's selected
        assert!(!result.selected(3));
    }

    #[test]
    fn test_or_with_nulls() {
        // Test Kleene OR logic: true OR null = true, false OR null = null

        // Case 1: FALSE mask OR mask with nulls
        let false_mask = NullableRowIdMask::BlockList(NullableRowAddrSet::new(
            RowAddrTreeMap::from_iter(&[0, 1, 2]), // All FALSE
            RowAddrTreeMap::new(),                 // No nulls
        ));
        let null_mask = NullableRowIdMask::AllowList(NullableRowAddrSet::new(
            RowAddrTreeMap::from_iter(&[0, 1, 2]), // TRUE or NULL
            RowAddrTreeMap::from_iter(&[1, 2]),    // NULL rows
        ));
        let result = false_mask | null_mask.clone();

        // FALSE OR TRUE = TRUE
        assert!(result.selected(0));
        // FALSE OR NULL = NULL (filtered out)
        assert!(!result.selected(1));
        assert!(!result.selected(2));

        // Case 2: TRUE mask OR mask with nulls
        let true_mask = NullableRowIdMask::AllowList(NullableRowAddrSet::new(
            RowAddrTreeMap::from_iter(&[0, 1, 2]), // All TRUE
            RowAddrTreeMap::new(),                 // No nulls
        ));
        let result = true_mask | null_mask;

        // TRUE OR anything = TRUE
        assert!(result.selected(0));
        assert!(result.selected(1));
        assert!(result.selected(2));

        // Case 3: Both have nulls
        let mask1 = NullableRowIdMask::BlockList(NullableRowAddrSet::new(
            RowAddrTreeMap::from_iter(&[0, 1, 2, 3]), // FALSE or NULL
            RowAddrTreeMap::from_iter(&[1, 2]),       // NULL rows
        ));
        let mask2 = NullableRowIdMask::BlockList(NullableRowAddrSet::new(
            RowAddrTreeMap::from_iter(&[0, 1, 2, 3]), // FALSE or NULL
            RowAddrTreeMap::from_iter(&[2, 3]),       // NULL rows
        ));
        let result = mask1 | mask2;

        // Row 0 is FALSE in both
        assert!(!result.selected(0));
        // Row 1 is NULL in first, FALSE in second -> NULL
        assert!(!result.selected(1));
        // Row 2 is NULL in both -> NULL
        assert!(!result.selected(2));
        // Row 3 is FALSE in first, NULL in second -> NULL
        assert!(!result.selected(3));
    }

    #[test]
    fn test_row_selection_bit_or() {
        // [T, N, T, N, F, F, F]
        let left = NullableRowAddrSet::new(
            RowAddrTreeMap::from_iter(&[1, 2, 3, 4]),
            RowAddrTreeMap::from_iter(&[2, 4]),
        );
        // [F, F, T, N, T, N, N]
        let right = NullableRowAddrSet::new(
            RowAddrTreeMap::from_iter(&[3, 4, 5, 6]),
            RowAddrTreeMap::from_iter(&[4, 6, 7]),
        );
        // [T, N, T, N, T, N, N]
        let expected_true = RowAddrTreeMap::from_iter(&[1, 3, 5]);
        let expected_nulls = RowAddrTreeMap::from_iter(&[2, 4, 6, 7]);

        let mut result = left.clone();
        result |= &right;
        assert_eq!(&result.true_rows(), &expected_true);
        assert_eq!(result.null_rows(), &expected_nulls);
        // Commutative property holds
        let mut result = right.clone();
        result |= &left;
        assert_eq!(&result.true_rows(), &expected_true);
        assert_eq!(result.null_rows(), &expected_nulls);
    }

    #[test]
    fn test_row_selection_bit_and() {
        // [T, N, T, N, F, F, F]
        let left = NullableRowAddrSet::new(
            RowAddrTreeMap::from_iter(&[1, 2, 3, 4]),
            RowAddrTreeMap::from_iter(&[2, 4]),
        );
        // [F, F, T, N, T, N, N]
        let right = NullableRowAddrSet::new(
            RowAddrTreeMap::from_iter(&[3, 4, 5, 6]),
            RowAddrTreeMap::from_iter(&[4, 6, 7]),
        );
        // [F, F, T, N, F, F, F]
        let expected_true = RowAddrTreeMap::from_iter(&[3]);
        let expected_nulls = RowAddrTreeMap::from_iter(&[4]);
        let mut result = left.clone();
        result &= &right;
        assert_eq!(&result.true_rows(), &expected_true);
        assert_eq!(result.null_rows(), &expected_nulls);
        // Commutative property holds
        let mut result = right.clone();
        result &= &left;
        assert_eq!(&result.true_rows(), &expected_true);
        assert_eq!(result.null_rows(), &expected_nulls);
    }

    #[test]
    fn test_union_all() {
        // Union all is basically a series of ORs.
        // [T, T, T, N, N, N, F, F, F]
        let set1 = NullableRowAddrSet::new(
            RowAddrTreeMap::from_iter(&[1, 2, 3, 4]),
            RowAddrTreeMap::from_iter(&[4, 5, 6]),
        );
        // [T, N, F, T, N, F, T, N, F]
        let set2 = NullableRowAddrSet::new(
            RowAddrTreeMap::from_iter(&[1, 4, 7, 8]),
            RowAddrTreeMap::from_iter(&[2, 5, 8]),
        );
        let set3 = NullableRowAddrSet::empty();

        let result = NullableRowAddrSet::union_all(&[set1, set2, set3]);

        // [T, T, T, T, N, N, T, N, F]
        let expected_true = RowAddrTreeMap::from_iter(&[1, 2, 3, 4, 7]);
        let expected_nulls = RowAddrTreeMap::from_iter(&[5, 6, 8]);

        assert_eq!(&result.true_rows(), &expected_true);
        assert_eq!(result.null_rows(), &expected_nulls);
    }
}
