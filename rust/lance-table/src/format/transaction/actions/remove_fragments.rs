// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! `RemoveFragments` — drops fragments by id or by criterion (`All`).

use std::collections::BTreeSet;
use std::sync::Arc;

use super::{
    Action, ActionOutput, FragmentRef, FragmentScope, FragmentSelector, ManifestMask, ManifestPath,
    TransactionError, TxnContext, WorkingState,
};
use crate::format::Fragment;

#[derive(Debug, Clone)]
pub struct RemoveFragments {
    pub selector: FragmentSelector,
    /// Concrete set, filled in by rebase when selector is `All`.
    resolved: Option<Vec<u32>>,
}

impl RemoveFragments {
    pub fn refs(refs: Vec<FragmentRef>) -> Self {
        Self {
            selector: FragmentSelector::Refs(refs),
            resolved: None,
        }
    }

    pub fn all() -> Self {
        Self {
            selector: FragmentSelector::All,
            resolved: None,
        }
    }

    /// Concrete fragment ids targeted by this action, if knowable without ctx.
    pub fn explicit_ids(&self) -> Option<Vec<u32>> {
        if let Some(resolved) = &self.resolved {
            return Some(resolved.clone());
        }
        if let FragmentSelector::Refs(refs) = &self.selector {
            let mut out = Vec::with_capacity(refs.len());
            for r in refs {
                if let FragmentRef::Existing(id) = r {
                    out.push(*id);
                } else {
                    return None;
                }
            }
            return Some(out);
        }
        None
    }
}

impl Action for RemoveFragments {
    fn reads(&self) -> ManifestMask {
        let mut mask = ManifestMask::singleton(ManifestPath::FragmentList);
        if let FragmentSelector::Refs(refs) = &self.selector {
            for r in refs {
                if let FragmentRef::Existing(id) = r {
                    mask.insert(ManifestPath::Fragment {
                        id: *id,
                        scope: FragmentScope::Whole,
                    });
                }
            }
        }
        mask
    }

    fn writes(&self) -> ManifestMask {
        self.reads()
    }

    fn validate(&self, state: &WorkingState) -> Result<(), TransactionError> {
        if let FragmentSelector::Refs(refs) = &self.selector {
            let ids: BTreeSet<u64> = state.manifest.fragments.iter().map(|f| f.id).collect();
            for r in refs {
                if let FragmentRef::Existing(id) = r
                    && !ids.contains(&(*id as u64))
                {
                    return Err(TransactionError::Incompatible(format!(
                        "RemoveFragments: fragment {id} not present"
                    )));
                }
            }
        }
        Ok(())
    }

    fn rebase(
        &mut self,
        current: &WorkingState,
        _concurrent: &[Box<dyn Action>],
    ) -> Result<(), TransactionError> {
        // Criterion-based: re-evaluate `All` against current state.
        if matches!(self.selector, FragmentSelector::All) {
            self.resolved = Some(
                current
                    .manifest
                    .fragments
                    .iter()
                    .map(|f| f.id as u32)
                    .collect(),
            );
        }
        Ok(())
    }

    fn apply(
        &self,
        state: &mut WorkingState,
        ctx: &mut TxnContext,
    ) -> Result<ActionOutput, TransactionError> {
        let want: BTreeSet<u64> = match (&self.selector, &self.resolved) {
            (_, Some(ids)) => ids.iter().map(|id| *id as u64).collect(),
            (FragmentSelector::All, None) => {
                state.manifest.fragments.iter().map(|f| f.id).collect()
            }
            (FragmentSelector::Refs(refs), None) => refs
                .iter()
                .map(|r| {
                    ctx.resolve(r).map(u64::from).ok_or_else(|| {
                        TransactionError::Incompatible(format!(
                            "RemoveFragments: unresolved ref {r:?}"
                        ))
                    })
                })
                .collect::<Result<_, _>>()?,
        };
        let before = state.manifest.fragments.len();
        let kept: Vec<Fragment> = state
            .manifest
            .fragments
            .iter()
            .filter(|f| !want.contains(&f.id))
            .cloned()
            .collect();
        if before - kept.len() != want.len() {
            return Err(TransactionError::Incompatible(format!(
                "RemoveFragments: not all targets present ({want:?})"
            )));
        }
        state.manifest.fragments = Arc::new(kept);
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
    fn remove_disjoint_fragments_both_apply() {
        let a: Box<dyn Action> = Box::new(RemoveFragments::refs(vec![
            FragmentRef::Existing(1),
            FragmentRef::Existing(2),
        ]));
        let b: Box<dyn Action> = Box::new(RemoveFragments::refs(vec![
            FragmentRef::Existing(3),
            FragmentRef::Existing(4),
        ]));
        assert!(a.writes().intersects(&b.writes())); // shared FragmentList
        let mut s = state_with_fragments(&[1, 2, 3, 4]);
        run_one(a, &mut s).unwrap();
        run_one(b, &mut s).unwrap();
        assert_eq!(s.manifest.fragments.len(), 0);
    }

    #[test]
    fn validate_catches_remove_of_missing_fragment() {
        let a = RemoveFragments::refs(vec![FragmentRef::Existing(99)]);
        let s = state_with_fragments(&[1, 2]);
        let err = a.validate(&s).unwrap_err();
        assert!(matches!(err, TransactionError::Incompatible(_)));
    }

    #[test]
    fn all_selector_resolves_at_rebase() {
        let mut a = RemoveFragments::all();
        let s = state_with_fragments(&[7, 8]);
        a.rebase(&s, &[]).unwrap();
        assert_eq!(a.resolved, Some(vec![7, 8]));
    }
}
