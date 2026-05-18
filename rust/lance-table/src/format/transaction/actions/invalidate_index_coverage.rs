// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! `InvalidateIndexCoverage` — prune fragments from any index over the named
//! fields (criterion-based, §3.1).

use roaring::RoaringBitmap;

use super::{
    Action, ActionOutput, IndexScope, ManifestMask, ManifestPath, TransactionError, TxnContext,
    WorkingState,
};

#[derive(Debug, Clone)]
pub struct InvalidateIndexCoverage {
    pub fragment_ids: Vec<u32>,
    pub field_ids: Vec<u32>,
}

impl Action for InvalidateIndexCoverage {
    fn reads(&self) -> ManifestMask {
        ManifestMask::singleton(ManifestPath::IndicesCoveringFields {
            fields: self.field_ids.clone(),
            scope: IndexScope::Whole,
        })
    }

    fn writes(&self) -> ManifestMask {
        ManifestMask::singleton(ManifestPath::IndicesCoveringFields {
            fields: self.field_ids.clone(),
            scope: IndexScope::Bitmap,
        })
    }

    fn validate(&self, _state: &WorkingState) -> Result<(), TransactionError> {
        Ok(())
    }

    fn rebase(
        &mut self,
        _current: &WorkingState,
        _concurrent: &[Box<dyn Action>],
    ) -> Result<(), TransactionError> {
        // Criterion-based: nothing to rewrite. apply re-evaluates against the
        // current index set, so a concurrent AddIndex that creates a matching
        // index is automatically covered.
        Ok(())
    }

    fn apply(
        &self,
        state: &mut WorkingState,
        _ctx: &mut TxnContext,
    ) -> Result<ActionOutput, TransactionError> {
        let drop = RoaringBitmap::from_iter(self.fragment_ids.iter().copied());
        let field_ids_i32: Vec<i32> = self.field_ids.iter().map(|f| *f as i32).collect();
        for idx in state.indices.iter_mut() {
            if !idx.fields.iter().any(|f| field_ids_i32.contains(f)) {
                continue;
            }
            if let Some(bitmap) = idx.fragment_bitmap.as_mut() {
                *bitmap -= &drop;
            }
        }
        Ok(ActionOutput::None)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::super::{IndexScope, ManifestPath};
    use super::*;

    #[test]
    fn criterion_mask_intersects_concrete_index_path() {
        let invalidate = InvalidateIndexCoverage {
            fragment_ids: vec![1],
            field_ids: vec![7],
        };
        let concrete = ManifestMask::singleton(ManifestPath::Index {
            uuid: [0u8; 16],
            scope: IndexScope::Whole,
        });
        assert!(invalidate.writes().intersects(&concrete));
    }

    #[test]
    fn criterion_masks_disjoint_when_fields_dont_overlap() {
        let a = InvalidateIndexCoverage {
            fragment_ids: vec![1],
            field_ids: vec![7],
        };
        let b = InvalidateIndexCoverage {
            fragment_ids: vec![1],
            field_ids: vec![8],
        };
        assert!(!a.writes().intersects(&b.writes()));
    }

    #[test]
    fn apply_drops_fragment_from_matching_index_bitmap() {
        let mut s = state_with_fragments(&[1, 2]);
        s.indices.push(crate::format::IndexMetadata {
            uuid: uuid::Uuid::nil(),
            fields: vec![7],
            name: "idx".into(),
            dataset_version: 0,
            fragment_bitmap: Some(RoaringBitmap::from_iter([1, 2])),
            index_details: None,
            index_version: 0,
            created_at: None,
            base_id: None,
            files: None,
        });
        let action = InvalidateIndexCoverage {
            fragment_ids: vec![1],
            field_ids: vec![7],
        };
        run_one(Box::new(action), &mut s).unwrap();
        let bm = s.indices[0].fragment_bitmap.as_ref().unwrap();
        assert!(!bm.contains(1));
        assert!(bm.contains(2));
    }
}
