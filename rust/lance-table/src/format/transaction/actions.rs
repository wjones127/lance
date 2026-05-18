// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Prototype: action-based transactions.
//!
//! See `rust/lance-table/design/action_based_conflict_resolution.md`.
//!
//! Lifecycle per action (§7 of the design):
//! - `validate(state)` — pure, no IO. Well-formedness against `state`.
//! - `rebase(current, concurrent)` — mutates self. Allowed to do IO in
//!   production; this prototype's impls are sync. Only called when the
//!   mask screen indicates overlap.
//! - `apply(state, ctx)` — pure, no IO. Validates inline, then mutates.
//!
//! Principles exercised:
//! - Mask as conservative screen (§3, §5).
//! - Criterion-based targeting (§3.1) — see `IndicesCoveringFields`,
//!   `FragmentSelector::All`.
//! - Late binding (§3.2) — `FragmentTemplate` has no ID.
//! - Symbolic intra-txn references (§3.3) — `FragmentRef::ProducedBy`.

use std::collections::BTreeSet;

use crate::format::{DataFile, DeletionFile, Fragment, IndexMetadata, Manifest};

pub mod add_fragments;
pub mod add_index;
pub mod change_schema;
pub mod invalidate_index_coverage;
pub mod remove_fragments;
pub mod update_deletion_vector;

#[cfg(test)]
mod conflict_tests;

#[allow(unused_imports)] // Re-exports; consumers live in tests for now.
pub use {
    add_fragments::AddFragments, add_index::AddIndex, change_schema::ChangeSchema,
    invalidate_index_coverage::InvalidateIndexCoverage, remove_fragments::RemoveFragments,
    update_deletion_vector::UpdateDeletionVector,
};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum TransactionError {
    Incompatible(String),
    Generic(String),
}

impl std::fmt::Display for TransactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Incompatible(m) => write!(f, "action incompatible with state: {m}"),
            Self::Generic(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for TransactionError {}

// ---------------------------------------------------------------------------
// Working state
// ---------------------------------------------------------------------------

/// Prototype-only wrapper for the mutable dataset state an action operates on.
/// In production this would be the `Manifest` plus whatever auxiliary stores
/// (index list, frag-reuse index, MemWAL state) the action layer needs to
/// reason about. Splitting it out here keeps the trait honest about the
/// mutation surface without forcing every example through the real on-disk
/// representation.
#[derive(Debug, Clone)]
pub struct WorkingState {
    pub manifest: Manifest,
    pub indices: Vec<IndexMetadata>,
}

impl WorkingState {
    pub fn new(manifest: Manifest) -> Self {
        Self {
            manifest,
            indices: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Manifest mask
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ManifestMask {
    paths: BTreeSet<ManifestPath>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ManifestPath {
    FragmentList,
    Fragment { id: u32, scope: FragmentScope },

    Index { uuid: [u8; 16], scope: IndexScope },
    IndicesCoveringFields { fields: Vec<u32>, scope: IndexScope },

    Schema,

    NextFragmentId,
    NextRowId,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum FragmentScope {
    Whole,
    DataFiles { fields: Option<Vec<u32>> },
    DeletionVector,
    RowIds,
    VersionMeta,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum IndexScope {
    Whole,
    Bitmap,
    Files,
}

impl ManifestMask {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn singleton(path: ManifestPath) -> Self {
        let mut paths = BTreeSet::new();
        paths.insert(path);
        Self { paths }
    }

    pub fn insert(&mut self, path: ManifestPath) {
        self.paths.insert(path);
    }

    pub fn union(mut self, other: Self) -> Self {
        self.paths.extend(other.paths);
        self
    }

    pub fn intersects(&self, other: &Self) -> bool {
        self.paths
            .iter()
            .any(|a| other.paths.iter().any(|b| paths_overlap(a, b)))
    }
}

fn paths_overlap(a: &ManifestPath, b: &ManifestPath) -> bool {
    use ManifestPath::*;
    match (a, b) {
        (FragmentList, FragmentList) => true,
        (NextFragmentId, NextFragmentId) => true,
        (NextRowId, NextRowId) => true,
        (Schema, Schema) => true,
        (Fragment { id: ia, scope: sa }, Fragment { id: ib, scope: sb }) => {
            ia == ib && fragment_scopes_overlap(sa, sb)
        }
        (
            Index {
                uuid: ua,
                scope: sa,
            },
            Index {
                uuid: ub,
                scope: sb,
            },
        ) => ua == ub && index_scopes_overlap(sa, sb),
        (
            IndicesCoveringFields {
                fields: fa,
                scope: sa,
            },
            IndicesCoveringFields {
                fields: fb,
                scope: sb,
            },
        ) => index_scopes_overlap(sa, sb) && fields_intersect(fa, fb),
        (IndicesCoveringFields { scope: sa, .. }, Index { scope: sb, .. })
        | (Index { scope: sb, .. }, IndicesCoveringFields { scope: sa, .. }) => {
            // Conservative: criterion path matches any concrete index when scopes overlap.
            index_scopes_overlap(sa, sb)
        }
        _ => false,
    }
}

fn fragment_scopes_overlap(a: &FragmentScope, b: &FragmentScope) -> bool {
    use FragmentScope::*;
    match (a, b) {
        (Whole, _) | (_, Whole) => true,
        (DataFiles { fields: fa }, DataFiles { fields: fb }) => match (fa, fb) {
            (None, _) | (_, None) => true,
            (Some(xs), Some(ys)) => xs.iter().any(|x| ys.contains(x)),
        },
        (DeletionVector, DeletionVector) => true,
        (RowIds, RowIds) => true,
        (VersionMeta, VersionMeta) => true,
        _ => false,
    }
}

fn index_scopes_overlap(a: &IndexScope, b: &IndexScope) -> bool {
    use IndexScope::*;
    matches!((a, b), (Whole, _) | (_, Whole)) || a == b
}

fn fields_intersect(a: &[u32], b: &[u32]) -> bool {
    a.iter().any(|x| b.contains(x))
}

// ---------------------------------------------------------------------------
// Templates and refs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct FragmentTemplate {
    pub files: Vec<DataFile>,
    pub physical_rows: Option<usize>,
    pub deletion_file: Option<DeletionFile>,
}

impl FragmentTemplate {
    pub(crate) fn materialize(&self, id: u64) -> Fragment {
        Fragment {
            id,
            files: self.files.clone(),
            deletion_file: self.deletion_file.clone(),
            row_id_meta: None,
            physical_rows: self.physical_rows,
            last_updated_at_version_meta: None,
            created_at_version_meta: None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum FragmentRef {
    Existing(u32),
    ProducedBy {
        action_idx: usize,
        output_idx: usize,
    },
}

/// Criterion for selecting fragments. Resolved at apply time against the
/// current manifest — `All` matches whatever fragments exist then.
#[derive(Debug, Clone)]
pub enum FragmentSelector {
    Refs(Vec<FragmentRef>),
    All,
}

// ---------------------------------------------------------------------------
// Action trait + driver context
// ---------------------------------------------------------------------------

pub trait Action: std::fmt::Debug {
    fn reads(&self) -> ManifestMask;
    fn writes(&self) -> ManifestMask;

    /// Pure. Is this action well-formed against `state`?
    fn validate(&self, state: &WorkingState) -> Result<(), TransactionError>;

    /// Mutates self to absorb concurrent changes. Sync in this prototype;
    /// the production trait would be async to allow IO. Only called when
    /// the mask screen indicates the action overlaps concurrent writes.
    fn rebase(
        &mut self,
        current: &WorkingState,
        concurrent: &[Box<dyn Action>],
    ) -> Result<(), TransactionError>;

    /// Pure. Validates inline, then applies.
    fn apply(
        &self,
        state: &mut WorkingState,
        ctx: &mut TxnContext,
    ) -> Result<ActionOutput, TransactionError>;

    /// Downcasting hook used by per-action rebase logic to inspect peers in
    /// the concurrent set. Each action overrides with `Some(self)` so callers
    /// can `downcast_ref` without depending on `Any` in the trait object's
    /// vtable from outside the prototype.
    fn as_any(&self) -> &dyn std::any::Any;
}

#[derive(Debug, Default)]
pub struct TxnContext {
    outputs: Vec<ActionOutput>,
}

impl TxnContext {
    pub fn fragment_ids_of(&self, action_idx: usize) -> &[u32] {
        match self.outputs.get(action_idx) {
            Some(ActionOutput::AddedFragmentIds(ids)) => ids,
            _ => &[],
        }
    }

    pub fn resolve(&self, r: &FragmentRef) -> Option<u32> {
        match r {
            FragmentRef::Existing(id) => Some(*id),
            FragmentRef::ProducedBy {
                action_idx,
                output_idx,
            } => self.fragment_ids_of(*action_idx).get(*output_idx).copied(),
        }
    }

    fn record(&mut self, output: ActionOutput) {
        self.outputs.push(output);
    }
}

#[derive(Debug, Clone)]
pub enum ActionOutput {
    None,
    AddedFragmentIds(Vec<u32>),
}

// ---------------------------------------------------------------------------
// Transaction driver
// ---------------------------------------------------------------------------

/// Apply a compound transaction. Returns the resulting state; the input is
/// cloned. Drops the working clone on failure.
pub fn commit(
    actions: &mut [Box<dyn Action>],
    latest: &WorkingState,
    intervening_mask: &ManifestMask,
    intervening_actions: &[Box<dyn Action>],
) -> Result<WorkingState, TransactionError> {
    let mut working = latest.clone();
    let mut ctx = TxnContext::default();

    for action in actions.iter_mut() {
        let touches = action.reads().union(action.writes());
        if intervening_mask.intersects(&touches) {
            action.rebase(&working, intervening_actions)?;
        }
        let output = action.apply(&mut working, &mut ctx)?;
        ctx.record(output);
    }

    Ok(working)
}

/// Helper for tests: union of every action's writes mask.
pub fn writes_mask_of(actions: &[Box<dyn Action>]) -> ManifestMask {
    actions
        .iter()
        .fold(ManifestMask::empty(), |acc, a| acc.union(a.writes()))
}

// ---------------------------------------------------------------------------
// Shared test helpers
// ---------------------------------------------------------------------------

#[cfg(test)]
pub mod test_support {
    use super::*;
    use crate::format::Fragment;
    use std::sync::Arc;

    pub fn empty_manifest() -> Manifest {
        let schema = lance_core::datatypes::Schema::default();
        Manifest::new(
            schema,
            Arc::new(vec![]),
            crate::format::DataStorageFormat::default(),
            std::collections::HashMap::new(),
        )
    }

    pub fn manifest_with_fragments(ids: &[u64]) -> Manifest {
        let mut m = empty_manifest();
        let frags: Vec<Fragment> = ids.iter().map(|id| Fragment::new(*id)).collect();
        m.max_fragment_id = ids.iter().copied().max().map(|x| x as u32);
        m.fragments = Arc::new(frags);
        m
    }

    pub fn state_with_fragments(ids: &[u64]) -> WorkingState {
        WorkingState::new(manifest_with_fragments(ids))
    }

    pub fn empty_template() -> FragmentTemplate {
        FragmentTemplate {
            files: vec![],
            physical_rows: Some(0),
            deletion_file: None,
        }
    }

    pub fn dummy_deletion_file() -> DeletionFile {
        DeletionFile {
            read_version: 0,
            id: 0,
            file_type: crate::format::DeletionFileType::Bitmap,
            num_deleted_rows: Some(0),
            base_id: None,
        }
    }

    pub fn run_one(
        action: Box<dyn Action>,
        state: &mut WorkingState,
    ) -> Result<(), TransactionError> {
        let mut actions = vec![action];
        let mask = ManifestMask::empty();
        let new = commit(&mut actions, state, &mask, &[])?;
        *state = new;
        Ok(())
    }
}
