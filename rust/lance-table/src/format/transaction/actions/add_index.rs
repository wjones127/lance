// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! `AddIndex` — register a new index over a set of fields.
//!
//! Payload includes the concrete `fragment_bitmap` captured at build time
//! (the writer has trained the index against those fragments). Rebase must
//! reconcile that bitmap with the current state: fragments that were
//! concurrently removed must be dropped; fragments that were concurrently
//! added are *not* automatically covered — the writer trained without them.
//!
//! Conflict with `Update`/`InvalidateIndexCoverage` over the same fields is
//! handled at the mask layer (criterion-based path intersects the new
//! concrete index path). When `AddIndex` lands first, a later
//! `InvalidateIndexCoverage` finds the new index at apply time and prunes
//! it. When `Update` lands first, `AddIndex.rebase` trims any removed
//! fragments and accepts (or fails) the new fragments per `on_new_fragments`.

use roaring::RoaringBitmap;
use uuid::Uuid;

use super::{
    Action, ActionOutput, IndexScope, ManifestMask, ManifestPath, TransactionError, TxnContext,
    WorkingState,
};
use crate::format::IndexMetadata;

/// What to do when concurrent writers added fragments after we trained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnNewFragments {
    /// Leave them out of coverage. Common case: a later `Optimize` will
    /// fold them in.
    LeaveUncovered,
    /// Fail — the writer demands full coverage of the latest fragments.
    Fail,
}

#[derive(Debug, Clone)]
pub struct AddIndex {
    pub uuid: Uuid,
    pub name: String,
    pub fields: Vec<u32>,
    pub fragment_bitmap: RoaringBitmap,
    pub on_new_fragments: OnNewFragments,
}

impl AddIndex {
    pub fn new(uuid: Uuid, name: impl Into<String>, fields: Vec<u32>, frags: &[u32]) -> Self {
        Self {
            uuid,
            name: name.into(),
            fields,
            fragment_bitmap: RoaringBitmap::from_iter(frags.iter().copied()),
            on_new_fragments: OnNewFragments::LeaveUncovered,
        }
    }
}

impl Action for AddIndex {
    fn reads(&self) -> ManifestMask {
        // The set of fragments we cover, plus the schema fields we index.
        ManifestMask::singleton(ManifestPath::FragmentList).union(ManifestMask::singleton(
            ManifestPath::IndicesCoveringFields {
                fields: self.fields.clone(),
                scope: IndexScope::Whole,
            },
        ))
    }

    fn writes(&self) -> ManifestMask {
        // Adding a new concrete index. Criterion-path readers (e.g.
        // InvalidateIndexCoverage on these fields) need to see this; the
        // mask layer's IndicesCoveringFields↔Index cross-rule handles it.
        ManifestMask::singleton(ManifestPath::Index {
            uuid: *self.uuid.as_bytes(),
            scope: IndexScope::Whole,
        })
        .union(ManifestMask::singleton(
            ManifestPath::IndicesCoveringFields {
                fields: self.fields.clone(),
                scope: IndexScope::Whole,
            },
        ))
    }

    fn validate(&self, state: &WorkingState) -> Result<(), TransactionError> {
        if state.indices.iter().any(|i| i.uuid == self.uuid) {
            return Err(TransactionError::Incompatible(format!(
                "AddIndex: uuid {} already registered",
                self.uuid
            )));
        }
        if state.indices.iter().any(|i| i.name == self.name) {
            return Err(TransactionError::Incompatible(format!(
                "AddIndex: name {:?} already in use",
                self.name
            )));
        }
        Ok(())
    }

    fn rebase(
        &mut self,
        current: &WorkingState,
        _concurrent: &[Box<dyn Action>],
    ) -> Result<(), TransactionError> {
        let current_frags: RoaringBitmap = current
            .manifest
            .fragments
            .iter()
            .map(|f| f.id as u32)
            .collect();

        let new_frags = &current_frags - &self.fragment_bitmap;
        if !new_frags.is_empty() && self.on_new_fragments == OnNewFragments::Fail {
            return Err(TransactionError::Incompatible(format!(
                "AddIndex: {} new fragments not covered by trained index",
                new_frags.len()
            )));
        }

        // Trim fragments that no longer exist (removed concurrently).
        self.fragment_bitmap &= &current_frags;
        Ok(())
    }

    fn apply(
        &self,
        state: &mut WorkingState,
        _ctx: &mut TxnContext,
    ) -> Result<ActionOutput, TransactionError> {
        self.validate(state)?;
        state.indices.push(IndexMetadata {
            uuid: self.uuid,
            fields: self.fields.iter().map(|f| *f as i32).collect(),
            name: self.name.clone(),
            dataset_version: state.manifest.version,
            fragment_bitmap: Some(self.fragment_bitmap.clone()),
            index_details: None,
            index_version: 0,
            created_at: None,
            base_id: None,
            files: None,
        });
        Ok(ActionOutput::None)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::*;

    #[test]
    fn add_index_writes_to_indices() {
        let mut s = state_with_fragments(&[1, 2]);
        let a = AddIndex::new(Uuid::nil(), "idx", vec![7], &[1, 2]);
        run_one(Box::new(a), &mut s).unwrap();
        assert_eq!(s.indices.len(), 1);
        assert_eq!(s.indices[0].fields, vec![7]);
    }

    #[test]
    fn duplicate_uuid_rejected() {
        let mut s = state_with_fragments(&[1]);
        let a = AddIndex::new(Uuid::nil(), "idx", vec![7], &[1]);
        run_one(Box::new(a), &mut s).unwrap();
        let b = AddIndex::new(Uuid::nil(), "idx2", vec![7], &[1]);
        let err = run_one(Box::new(b), &mut s).unwrap_err();
        assert!(matches!(err, TransactionError::Incompatible(_)));
    }

    #[test]
    fn rebase_trims_removed_fragments() {
        let s = state_with_fragments(&[1, 3]); // 2 has been removed concurrently
        let mut a = AddIndex::new(Uuid::nil(), "idx", vec![7], &[1, 2, 3]);
        a.rebase(&s, &[]).unwrap();
        assert_eq!(a.fragment_bitmap, RoaringBitmap::from_iter([1u32, 3]));
    }

    #[test]
    fn rebase_fail_mode_rejects_new_fragments() {
        let s = state_with_fragments(&[1, 2, 9]);
        let mut a = AddIndex::new(Uuid::nil(), "idx", vec![7], &[1, 2]);
        a.on_new_fragments = OnNewFragments::Fail;
        let err = a.rebase(&s, &[]).unwrap_err();
        assert!(matches!(err, TransactionError::Incompatible(_)));
    }
}
