// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Cross-action conflict resolution tests.
//!
//! Each test exercises the full `commit` driver: it stages a baseline state,
//! commits transaction A, then commits transaction B against the post-A
//! state with A's writes mask passed as the intervening mask. This is the
//! shape a production conflict resolver would use — the question every test
//! asks is "did the driver correctly accept, merge, or reject B?"
//!
//! Scenarios covered:
//! - Update || Update on different fragments  → compatible
//! - Update || Update on same fragment, different rows → compatible (UDVs
//!   would merge by union; prototype skips the IO)
//! - Update || Update on same fragment, same row → conflict
//! - Update || CreateIndex (both orderings) — exercises the criterion-based
//!   path crossing the new concrete index.

use roaring::RoaringBitmap;
use uuid::Uuid;

use super::test_support::*;
use super::{
    Action, AddFragments, AddIndex, FragmentRef, FragmentTemplate, InvalidateIndexCoverage,
    ManifestMask, RemoveFragments, TransactionError, UpdateDeletionVector, WorkingState, commit,
    writes_mask_of,
};
use crate::format::DeletionFile;

// ---------------------------------------------------------------------------
// Update-as-actions helpers
// ---------------------------------------------------------------------------

/// Update via RewriteRows decomposition: remove old fragment, add the
/// replacement, and (for any field covered by an index) invalidate that
/// fragment's coverage. Returns the action list ready to feed `commit`.
fn update_rewrite_rows(old_frag: u32, indexed_fields: &[u32]) -> Vec<Box<dyn Action>> {
    let mut actions: Vec<Box<dyn Action>> = vec![
        Box::new(RemoveFragments::refs(vec![FragmentRef::Existing(old_frag)])),
        Box::new(AddFragments {
            fragments: vec![empty_template()],
        }),
    ];
    if !indexed_fields.is_empty() {
        actions.push(Box::new(InvalidateIndexCoverage {
            fragment_ids: vec![old_frag],
            field_ids: indexed_fields.to_vec(),
        }));
    }
    actions
}

/// Update via partial delete: just an UpdateDeletionVector.
fn update_partial_delete(frag: u32, affected_rows: &[u32]) -> Vec<Box<dyn Action>> {
    vec![Box::new(UpdateDeletionVector {
        fragment: FragmentRef::Existing(frag),
        new_deletion_file: DeletionFile {
            read_version: 0,
            id: 0,
            file_type: crate::format::DeletionFileType::Bitmap,
            num_deleted_rows: Some(affected_rows.len()),
            base_id: None,
        },
        affected_rows: Some(RoaringBitmap::from_iter(affected_rows.iter().copied())),
    })]
}

fn run_two_concurrent(
    base: WorkingState,
    mut a: Vec<Box<dyn Action>>,
    mut b: Vec<Box<dyn Action>>,
) -> Result<(WorkingState, WorkingState), TransactionError> {
    let s1 = commit(&mut a, &base, &ManifestMask::empty(), &[]).unwrap();
    let intervening = writes_mask_of(&a);
    let s2 = commit(&mut b, &s1, &intervening, &a)?;
    Ok((s1, s2))
}

// ---------------------------------------------------------------------------
// Update || Update
// ---------------------------------------------------------------------------

/// Different fragments: A rewrites F1, B rewrites F2. No row overlap.
/// Mask check shows that both touch FragmentList (via RemoveFragments and
/// AddFragments), but the per-fragment paths are disjoint and AddFragments
/// is rebase-tolerant (late-bound IDs). Both succeed; the final state has
/// neither F1 nor F2 but two new fragments.
#[test]
fn update_update_different_fragments_succeeds() {
    let base = state_with_fragments(&[1, 2]);
    let a = update_rewrite_rows(1, &[]);
    let b = update_rewrite_rows(2, &[]);
    let (_, s2) = run_two_concurrent(base, a, b).unwrap();
    let ids: Vec<u64> = s2.manifest.fragments.iter().map(|f| f.id).collect();
    // Both originals gone; two new ones added with fresh IDs.
    assert!(!ids.contains(&1));
    assert!(!ids.contains(&2));
    assert_eq!(ids.len(), 2);
}

/// Same fragment, different rows: A deletes row 0 on F1, B deletes row 5
/// on F1. UDV rebase sees disjoint affected_rows on the same fragment and
/// allows the merge (real impl would write a unioned deletion file).
#[test]
fn update_update_same_fragment_different_rows_merges() {
    let base = state_with_fragments(&[1]);
    let a = update_partial_delete(1, &[0, 1]);
    let b = update_partial_delete(1, &[5, 6]);
    let (_, s2) = run_two_concurrent(base, a, b).unwrap();
    // B's UDV wrote through; the prototype's apply replaces rather than
    // unions, so the assertion here is just "no error and the fragment
    // is still present."
    assert_eq!(s2.manifest.fragments.len(), 1);
}

/// Same fragment, same row: A deletes row 5, B deletes rows 3-7. Rebase
/// detects the overlap and refuses to commit B.
#[test]
fn update_update_same_row_conflicts() {
    let base = state_with_fragments(&[1]);
    let a = update_partial_delete(1, &[5]);
    let b = update_partial_delete(1, &[3, 4, 5, 6, 7]);
    let err = run_two_concurrent(base, a, b).unwrap_err();
    assert!(
        matches!(err, TransactionError::Incompatible(_)),
        "expected row-level conflict, got {err:?}"
    );
}

/// Same fragment, full rewrite vs. full rewrite: A and B both
/// RewriteRows F1. RemoveFragments(F1) collides with RemoveFragments(F1):
/// the second one finds F1 already gone at apply time.
#[test]
fn update_update_same_fragment_full_rewrite_conflicts() {
    let base = state_with_fragments(&[1]);
    let a = update_rewrite_rows(1, &[]);
    let b = update_rewrite_rows(1, &[]);
    let err = run_two_concurrent(base, a, b).unwrap_err();
    assert!(matches!(err, TransactionError::Incompatible(_)));
}

// ---------------------------------------------------------------------------
// Update || CreateIndex
// ---------------------------------------------------------------------------

/// AddIndex commits first; Update then runs. The Update's
/// InvalidateIndexCoverage is criterion-based (over field 7) — it doesn't
/// need to know A added an index. At apply time it walks the index list
/// and prunes the updated fragment from the bitmap.
#[test]
fn create_index_then_update_invalidates_coverage() {
    let base = state_with_fragments(&[1, 2, 3]);
    let a: Vec<Box<dyn Action>> = vec![Box::new(AddIndex::new(
        Uuid::from_u128(1),
        "idx_x",
        vec![7],
        &[1, 2, 3],
    ))];
    let b = update_rewrite_rows(2, &[7]); // F2 has field 7

    let (_, s2) = run_two_concurrent(base, a, b).unwrap();
    assert_eq!(s2.indices.len(), 1);
    let bm = s2.indices[0].fragment_bitmap.as_ref().unwrap();
    assert!(!bm.contains(2), "F2 should be invalidated, got {bm:?}");
    assert!(bm.contains(1));
    assert!(bm.contains(3));
}

/// Update commits first; AddIndex's bitmap was trained against the
/// pre-update fragments (F1..F3). Rebase trims F2 (now removed) from the
/// trained bitmap. The newly added replacement fragment is left
/// uncovered — an Optimize later would fold it in.
#[test]
fn update_then_create_index_trims_removed_fragments() {
    let base = state_with_fragments(&[1, 2, 3]);
    let a = update_rewrite_rows(2, &[]); // no index existed yet, so no invalidate
    let b: Vec<Box<dyn Action>> = vec![Box::new(AddIndex::new(
        Uuid::from_u128(2),
        "idx_x",
        vec![7],
        &[1, 2, 3], // trained when F2 was still around
    ))];

    let (_, s2) = run_two_concurrent(base, a, b).unwrap();
    assert_eq!(s2.indices.len(), 1);
    let bm = s2.indices[0].fragment_bitmap.as_ref().unwrap();
    assert!(!bm.contains(2));
    assert!(bm.contains(1));
    assert!(bm.contains(3));
}

/// Two AddIndex on overlapping fields with distinct UUIDs: not a conflict.
/// Both indices land in the index list independently.
#[test]
fn two_create_indices_same_field_both_land() {
    let base = state_with_fragments(&[1, 2]);
    let a: Vec<Box<dyn Action>> = vec![Box::new(AddIndex::new(
        Uuid::from_u128(10),
        "idx_a",
        vec![7],
        &[1, 2],
    ))];
    let b: Vec<Box<dyn Action>> = vec![Box::new(AddIndex::new(
        Uuid::from_u128(11),
        "idx_b",
        vec![7],
        &[1, 2],
    ))];
    let (_, s2) = run_two_concurrent(base, a, b).unwrap();
    assert_eq!(s2.indices.len(), 2);
}

// ---------------------------------------------------------------------------
// Carryover: Overwrite || Overwrite (kept from the earlier prototype).
// ---------------------------------------------------------------------------

#[test]
fn concurrent_overwrite_last_writer_wins() {
    use super::ChangeSchema;
    let schema_a = lance_core::datatypes::Schema::default();
    let schema_b = lance_core::datatypes::Schema::default();

    let s0 = WorkingState::new(empty_manifest());

    let mut a: Vec<Box<dyn Action>> = vec![
        Box::new(RemoveFragments::all()),
        Box::new(ChangeSchema {
            new_schema: schema_a,
        }),
        Box::new(AddFragments {
            fragments: vec![empty_template()],
        }),
    ];
    let s1 = commit(&mut a, &s0, &ManifestMask::empty(), &[]).unwrap();
    let frag_a_id = s1.manifest.fragments[0].id;

    let intervening = writes_mask_of(&a);

    let mut b: Vec<Box<dyn Action>> = vec![
        Box::new(RemoveFragments::all()),
        Box::new(ChangeSchema {
            new_schema: schema_b,
        }),
        Box::new(AddFragments {
            fragments: vec![empty_template()],
        }),
    ];
    let s2 = commit(&mut b, &s1, &intervening, &a).unwrap();

    assert_eq!(s2.manifest.fragments.len(), 1);
    assert_ne!(s2.manifest.fragments[0].id, frag_a_id);
    assert!(s2.manifest.max_fragment_id.unwrap() as u64 >= frag_a_id);
}

// Unused-but-illustrative: `FragmentTemplate` import keeps doc cross-refs
// honest if a future test wants to build a template inline.
#[allow(dead_code)]
fn _silence_unused_template_import(_: FragmentTemplate) {}
