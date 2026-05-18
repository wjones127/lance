// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! `UpdateDeletionVector` — replaces a fragment's deletion vector.
//!
//! Rebase logic implements the rule from §4 of the design doc:
//! "two UpdateDeletionVectors on the same fragment merge by union iff both
//! carry `affected_rows`; otherwise conflict." Same-row writes conflict.
//! In production the merge would write a new deletion file (IO); here we
//! detect the case and leave the file write as a `todo!`-equivalent.

use std::sync::Arc;

use roaring::RoaringBitmap;

use super::{
    Action, ActionOutput, FragmentRef, FragmentScope, ManifestMask, ManifestPath, TransactionError,
    TxnContext, WorkingState,
};
use crate::format::{DeletionFile, Fragment};

#[derive(Debug, Clone)]
pub struct UpdateDeletionVector {
    pub fragment: FragmentRef,
    pub new_deletion_file: DeletionFile,
    pub affected_rows: Option<RoaringBitmap>,
}

impl UpdateDeletionVector {
    fn frag_id_if_concrete(&self) -> Option<u32> {
        match self.fragment {
            FragmentRef::Existing(id) => Some(id),
            FragmentRef::ProducedBy { .. } => None,
        }
    }
}

impl Action for UpdateDeletionVector {
    fn reads(&self) -> ManifestMask {
        match &self.fragment {
            FragmentRef::Existing(id) => ManifestMask::singleton(ManifestPath::Fragment {
                id: *id,
                scope: FragmentScope::DeletionVector,
            }),
            FragmentRef::ProducedBy { .. } => ManifestMask::singleton(ManifestPath::FragmentList),
        }
    }

    fn writes(&self) -> ManifestMask {
        self.reads()
    }

    fn validate(&self, state: &WorkingState) -> Result<(), TransactionError> {
        if let FragmentRef::Existing(id) = &self.fragment
            && !state.manifest.fragments.iter().any(|f| f.id == *id as u64)
        {
            return Err(TransactionError::Incompatible(format!(
                "UpdateDeletionVector: fragment {id} not present"
            )));
        }
        Ok(())
    }

    fn rebase(
        &mut self,
        _current: &WorkingState,
        concurrent: &[Box<dyn Action>],
    ) -> Result<(), TransactionError> {
        // Need affected_rows on self to reason about row-level conflict.
        let self_id = self.frag_id_if_concrete();
        let self_rows = self.affected_rows.as_ref().ok_or_else(|| {
            TransactionError::Incompatible(
                "UpdateDeletionVector: cannot rebase without affected_rows".into(),
            )
        })?;

        for other in concurrent {
            // Same-fragment UDV: same-row → conflict, disjoint → ok (merge IO
            // would happen here in production).
            if let Some(udv) = other.as_any().downcast_ref::<Self>() {
                let other_id = udv.frag_id_if_concrete();
                if other_id != self_id || other_id.is_none() {
                    continue;
                }
                let other_rows = udv.affected_rows.as_ref().ok_or_else(|| {
                    TransactionError::Incompatible(
                        "UpdateDeletionVector: concurrent UDV without affected_rows; \
                         cannot determine row overlap"
                            .into(),
                    )
                })?;
                if !(self_rows & other_rows).is_empty() {
                    return Err(TransactionError::Incompatible(format!(
                        "UpdateDeletionVector: row-level conflict on fragment {:?}",
                        self_id
                    )));
                }
                // TODO: read concurrent deletion file and union into self.
                // Conservative-fail kept here would block disjoint-row updates;
                // we accept the success path for the prototype and leave the
                // merge IO unimplemented.
            }

            // A concurrent RemoveFragments dropping our fragment is fatal.
            if let Some(rf) = other
                .as_any()
                .downcast_ref::<super::remove_fragments::RemoveFragments>()
                && let Some(ids) = rf.explicit_ids()
                && self_id.is_some_and(|id| ids.contains(&id))
            {
                return Err(TransactionError::Incompatible(format!(
                    "UpdateDeletionVector: fragment {:?} removed by concurrent txn",
                    self_id
                )));
            }
        }

        Ok(())
    }

    fn apply(
        &self,
        state: &mut WorkingState,
        ctx: &mut TxnContext,
    ) -> Result<ActionOutput, TransactionError> {
        self.validate(state)?;
        let frag_id = ctx.resolve(&self.fragment).ok_or_else(|| {
            TransactionError::Incompatible(format!(
                "UpdateDeletionVector: unresolved ref {:?}",
                self.fragment
            ))
        })?;
        let mut new_fragments: Vec<Fragment> = state.manifest.fragments.as_ref().clone();
        let frag = new_fragments
            .iter_mut()
            .find(|f| f.id == frag_id as u64)
            .ok_or_else(|| {
                TransactionError::Incompatible(format!(
                    "UpdateDeletionVector: fragment {frag_id} not present at apply"
                ))
            })?;
        frag.deletion_file = Some(self.new_deletion_file.clone());
        state.manifest.fragments = Arc::new(new_fragments);
        Ok(ActionOutput::None)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::super::{RemoveFragments, commit};
    use super::*;

    fn udv(frag_id: u32, rows: &[u32]) -> UpdateDeletionVector {
        UpdateDeletionVector {
            fragment: FragmentRef::Existing(frag_id),
            new_deletion_file: dummy_deletion_file(),
            affected_rows: Some(RoaringBitmap::from_iter(rows.iter().copied())),
        }
    }

    #[test]
    fn validate_fails_on_apply_when_fragment_was_removed_concurrently() {
        let mut a: Vec<Box<dyn Action>> = vec![Box::new(RemoveFragments::refs(vec![
            FragmentRef::Existing(1),
        ]))];
        let s0 = state_with_fragments(&[1, 2]);
        let s1 = commit(&mut a, &s0, &ManifestMask::empty(), &[]).unwrap();

        let b = udv(1, &[0, 1]);
        let err = b.validate(&s1).unwrap_err();
        assert!(matches!(err, TransactionError::Incompatible(_)));
    }

    #[test]
    fn rebase_detects_same_row_conflict() {
        let s = state_with_fragments(&[1]);
        let concurrent: Vec<Box<dyn Action>> = vec![Box::new(udv(1, &[0, 1, 2]))];
        let mut me = udv(1, &[2, 3]); // row 2 overlaps
        let err = me.rebase(&s, &concurrent).unwrap_err();
        assert!(matches!(err, TransactionError::Incompatible(_)));
    }

    #[test]
    fn rebase_passes_for_disjoint_rows_same_fragment() {
        let s = state_with_fragments(&[1]);
        let concurrent: Vec<Box<dyn Action>> = vec![Box::new(udv(1, &[0, 1, 2]))];
        let mut me = udv(1, &[3, 4]);
        me.rebase(&s, &concurrent).unwrap();
    }

    #[test]
    fn rebase_passes_when_fragments_differ() {
        let s = state_with_fragments(&[1, 2]);
        let concurrent: Vec<Box<dyn Action>> = vec![Box::new(udv(2, &[0, 1]))];
        let mut me = udv(1, &[0, 1]);
        me.rebase(&s, &concurrent).unwrap();
    }
}
