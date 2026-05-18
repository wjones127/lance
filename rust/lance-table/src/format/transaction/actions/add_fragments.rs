// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! `AddFragments` — appends new fragments with late-bound IDs.

use std::sync::Arc;

use super::{
    Action, ActionOutput, FragmentTemplate, ManifestMask, ManifestPath, TransactionError,
    TxnContext, WorkingState,
};
use crate::format::Fragment;

#[derive(Debug, Clone)]
pub struct AddFragments {
    pub fragments: Vec<FragmentTemplate>,
}

impl Action for AddFragments {
    fn reads(&self) -> ManifestMask {
        ManifestMask::singleton(ManifestPath::NextFragmentId)
    }

    fn writes(&self) -> ManifestMask {
        ManifestMask::singleton(ManifestPath::FragmentList)
            .union(ManifestMask::singleton(ManifestPath::NextFragmentId))
    }

    fn validate(&self, _state: &WorkingState) -> Result<(), TransactionError> {
        // Real impl would check that each template's DataFiles reference
        // field IDs present in state.manifest.schema. Out of scope here.
        Ok(())
    }

    fn rebase(
        &mut self,
        _current: &WorkingState,
        _concurrent: &[Box<dyn Action>],
    ) -> Result<(), TransactionError> {
        // Late-bound outputs: nothing to rewrite. IDs allocated in apply.
        Ok(())
    }

    fn apply(
        &self,
        state: &mut WorkingState,
        _ctx: &mut TxnContext,
    ) -> Result<ActionOutput, TransactionError> {
        let mut next = state.manifest.max_fragment_id.map_or(0u32, |m| m + 1);
        let mut new_fragments: Vec<Fragment> = state.manifest.fragments.as_ref().clone();
        let mut assigned = Vec::with_capacity(self.fragments.len());
        for tpl in &self.fragments {
            let id = next;
            next += 1;
            assigned.push(id);
            new_fragments.push(tpl.materialize(id as u64));
        }
        state.manifest.max_fragment_id = Some(next.saturating_sub(1));
        state.manifest.fragments = Arc::new(new_fragments);
        Ok(ActionOutput::AddedFragmentIds(assigned))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::super::{FragmentRef, RemoveFragments, commit};
    use super::*;

    #[test]
    fn add_fragments_concurrent_get_distinct_ids() {
        let mut a: Vec<Box<dyn Action>> = vec![Box::new(AddFragments {
            fragments: vec![empty_template()],
        })];
        let mut b: Vec<Box<dyn Action>> = vec![Box::new(AddFragments {
            fragments: vec![empty_template()],
        })];

        let s0 = state_with_fragments(&[5]);
        let s1 = commit(&mut a, &s0, &ManifestMask::empty(), &[]).unwrap();
        let mask_after_a = a[0].writes();
        let s2 = commit(&mut b, &s1, &mask_after_a, &[]).unwrap();

        let ids: Vec<u64> = s2.manifest.fragments.iter().map(|f| f.id).collect();
        assert_eq!(ids, vec![5, 6, 7]);
    }

    #[test]
    fn symbolic_ref_resolves_within_compound_txn() {
        let mut actions: Vec<Box<dyn Action>> = vec![
            Box::new(AddFragments {
                fragments: vec![empty_template(), empty_template()],
            }),
            Box::new(RemoveFragments::refs(vec![FragmentRef::ProducedBy {
                action_idx: 0,
                output_idx: 0,
            }])),
        ];
        let s = state_with_fragments(&[10]);
        let s = commit(&mut actions, &s, &ManifestMask::empty(), &[]).unwrap();
        let ids: Vec<u64> = s.manifest.fragments.iter().map(|f| f.id).collect();
        assert_eq!(ids, vec![10, 12]);
    }
}
