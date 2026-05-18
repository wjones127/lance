# Action-Based Conflict Resolution

## 1. Background & Motivation

Today, conflict resolution lives in per-`Operation` methods in
`rust/lance/src/io/commit/conflict_resolver.rs` (`check_delete_txn`,
`check_rewrite_txn`, `check_create_index_txn`, ...). Each method has full
knowledge of the semantic intent of the `Operation` it handles.

Under action-based transactions, a transaction is a flat list of `Action`s
with no enclosing operation. Conflict detection and rebasing must work
from `Action` type + payload alone.

### Isolation level — current behavior

We have not been rigorous about specifying an isolation level. This section
documents what we actually do today (per-operation, by inspection) so we can
ask whether action-based resolution can preserve it — and what it would take
to offer stronger or weaker levels later.

TODO: characterize current behavior per operation. Likely somewhere between
snapshot isolation and serializable, with operation-specific carve-outs.

### Goal of this doc

1. Show whether conflict detection is possible purely from `Action` type +
   payload (no semantic intent).
2. Specify the conflict matrix and rebasing strategy.
3. Call out what each `Action` payload must carry to make (1) work.
4. Leave the door open to multiple isolation levels in the future.

## 2. Scope & Non-Goals

In scope:
- Conflict detection between concurrent transactions.
- Rebasing one transaction onto another.
- Compound transactions (multi-action, with intra-txn dependencies).

Out of scope:
- Physical commit protocol / manifest layout.
- Retry policy and backoff.
- API surface for submitting transactions.

## 3. Design Principles

Two patterns govern how actions are shaped. Both exist to avoid committing
too early to concrete values that may not survive concurrent commits.

### 3.1 Criterion-based targeting

Some actions act on an *open set* — entities a writer may not be able to
enumerate when the transaction is constructed, because a concurrent writer
can introduce new matching entities before commit.

Example: a transaction updates column X on fragment 5. Concurrently another
writer creates a new index over column X. Both land. If the first writer
named indices by UUID, the new index would end up covering stale data.

Rule: such actions carry a **predicate**, not an identity list. At
`try_apply` time the predicate is evaluated against the *current* manifest
and every matching entity is acted on. `InvalidateIndexCoverage` carries
`{ fragment_ids, field_ids }`, not index UUIDs.

Three tiers of consistency, with different homes:

| Tier | Example | Enforced by |
|------|---------|-------------|
| Local | "this fragment exists" | `try_apply` |
| Closed-set cross-resource | schema ↔ data files in the same txn | Builder at write time |
| Open-set cross-resource | column update ↔ any index over that column | Criterion-based actions |

### 3.2 Late binding of action output

Some actions *produce* new entities (fragments, row IDs, index UUIDs).
Their payload should declare the intent to produce, not the concrete
identity. IDs are assigned at apply time from the manifest's counters.

Contrast with a "rebasable IDs" design where the payload carries a
provisional ID that gets rewritten on rebase. Late binding is preferred
because:

- Transactions are serialized for conflict resolution and recovery.
  Carrying provisional IDs means the serialized form contains state that
  is stale on read.
- Rebase becomes a no-op for these fields — there is nothing to rewrite.

Type-level consequence: payloads use `FragmentTemplate` (no ID) rather
than `Fragment`. `apply` materializes the concrete `Fragment`.

Late binding applies to **outputs the action produces** (allocated IDs,
counter values). It does *not* apply to **inputs the action depends on**
(e.g. which rows it intends to mark deleted) — those are mutated by
`rebase` when concurrent changes shift them. See §7.

### 3.3 Symbolic intra-transaction references

A direct consequence of 3.2. If action *i* produces a fragment and action
*j* > *i* needs to reference it, *j* cannot use a concrete ID. References
are symbolic: `FragmentRef::ProducedBy { action_idx, output_idx }`,
resolved as actions are applied in order. This pins the compound-txn
dependency model to an **ordered list**, not a DAG.

## 4. Action Catalog

Enumerate each `Action` with:

- Payload fields
- Read-set (state it depends on)
- Write-set (state it modifies)
- Commutativity notes

* RemoveFragments { fragment_ids: Vec<u32> }
    - Read: fragment metadata (e.g. row counts, field coverage)
    - Write: fragment metadata, row addresses (implicitly)
    - Commutativity: commutes with non-overlapping RemoveFragments; conflicts with
        any action that reads/writes the same fragment metadata or row addresses
        (e.g. Update, AddIndex referencing those fragments)

* AddFragments { fragments: Vec<FragmentTemplate>,
                  preserved_row_ids: Option<Vec<PreservedRows>> }
    - `FragmentTemplate` carries data files / row counts / deletion file —
      *no* fragment ID and *no* row ID metadata (§3.2 late binding).
    - `preserved_row_ids` (optional) names source `(fragment_id, offset_range)`
      whose row IDs and `created_at_version_meta` should be carried into
      the new fragments — used by Update RewriteRows.
    - Read: NextFragmentId counter, NextRowId counter (if stable IDs on),
      source fragments named in `preserved_row_ids` (if any).
    - Write: fragment list (append), NextFragmentId, NextRowId.
    - Commutativity: commutes with anything that does not read the new
      fragments. Two concurrent AddFragments intersect on the counters →
      slow path → `try_apply` assigns fresh IDs.

* UpdateDeletionVector { fragment_id: u32, new_deletion_file: DeletionFile,
                         affected_rows: Option<RowAddressRanges> }
    - Read: fragment row count, existing deletion file
    - Write: fragment.deletion_file
    - Commutativity: commutes on different fragments. On the same fragment,
      two UpdateDeletionVectors merge by union iff both carry `affected_rows`
      and data files are unchanged; otherwise conflict.
    - Conflicts with: RemoveFragments(same id), ReplaceFragmentColumns(same id)
      when the affected rows overlap modified offsets, RewriteFragments(same id),
      Restore.

* ReplaceFragmentColumns { fragment_id: u32, field_ids: Vec<u32>,
                            new_data_files: Vec<DataFile>,
                            updated_row_offsets: Option<RowOffsetBitmap> }
    - Read: existing data files for these fields on this fragment
    - Write: data_files for listed fields; last_updated_at_version for rows
      in updated_row_offsets (or all rows if absent)
    - Commutativity: commutes with ReplaceFragmentColumns on disjoint
      (fragment_id, field_id) pairs. Conflicts with any reader/writer of
      those fields on that fragment.
    - Used by: Update RewriteColumns mode.

* RewriteFragments { old_fragment_ids: Vec<u32>,
                     new_fragments: Vec<FragmentTemplate>,
                     preserves_row_ids: bool }
    - Read: full contents of old fragments
    - Write: fragment list (splice old → new). Row addresses change. Row IDs
      preserved iff `preserves_row_ids`.
    - Used by: Compact. Distinguished from RemoveFragments+AddFragments
      because it asserts data equivalence — lets concurrent reads/index
      coverage be salvaged via RebindIndexCoverage.
    - Conflicts with: anything touching old_fragment_ids' data or deletion
      vectors; another RewriteFragments overlapping the same set.

* RebindIndexCoverage { remap: Vec<(old_frag_id, new_frag_id)>,
                         preserved_fields: Vec<u32> }
    - Read: index fragment_bitmap, index field membership
    - Write: index fragment_bitmap (swap old IDs for new IDs) for indices
      covering only `preserved_fields`
    - Used by: Compact, Update RewriteRows in the pure-rewrite (no inserts,
      no field changes) case.
    - Conflicts with: AddIndex / RewriteIndex over a bitmap that straddles
      a remap group; InvalidateIndexCoverage on the same (frag, field).

* InvalidateIndexCoverage { fragment_ids: Vec<u32>, field_ids: Vec<u32> }
    - Read: index field membership
    - Write: remove listed fragments from fragment_bitmap of every index
      covering any of `field_ids`
    - Used by: Update — for each fragment whose `fields_modified` overlap
      an index.

* RewriteIndex { old_id: Uuid, new_id: Uuid, new_index_files,
                  new_index_details, new_index_version }
    - Read: existing index entry
    - Write: replace index entry (uuid + files)
    - Used by: Compact without stable row IDs (full index rebuild).
    - Conflicts with: any concurrent RewriteIndex/AddIndex/DropIndex on the
      same UUID.

* RegisterFragReuse { entries: ... }
    - Read: current frag_reuse_index
    - Write: frag_reuse_index (deferred remap state)
    - Used by: Compact with stable row IDs.
    - Conflicts with: another RegisterFragReuse in the same commit window
      (only one deferred remap can land at a time); AddIndex whose coverage
      straddles a deferred-remap group.

* UpdateMergedGenerations { generations: Vec<MergedGeneration> }
    - Read: MemWAL index state
    - Write: MemWAL index entry
    - Used by: Update (when consuming a MemWAL generation), MemWAL flush.
    - Conflicts with: another UpdateMergedGenerations touching the same
      generation.

### Operation → Action decomposition (Delete / Update / Compact)

**Delete** =
- `RemoveFragments` over fully-emptied fragments, plus
- one `UpdateDeletionVector` per partially-deleted fragment.

**Update (RewriteRows)** =
- `RemoveFragments(removed_fragment_ids)` +
- `AddFragments(new_fragments)` (carrying preserved row IDs and
  created_at_version_meta for moved rows) +
- `UpdateDeletionVector(updated_fragments)` for any leftover partial deletes +
- For each fragment whose modified fields overlap an index:
  `InvalidateIndexCoverage`. For pure rewrites on preserved fields:
  `RebindIndexCoverage(old → new, preserved_fields)`. +
- Optional `UpdateMergedGenerations`.
- Merge-insert primary-key conflict detection (`inserted_rows_filter`) is
  attached to AddFragments as conflict-detection metadata — see §7.

**Update (RewriteColumns)** =
- One `ReplaceFragmentColumns` per (fragment, field-set) pair +
- `InvalidateIndexCoverage` for indices over those fields on those fragments.

**Compact** =
- One `RewriteFragments` per RewriteGroup +
- With stable row IDs: `RebindIndexCoverage` for every index whose bitmap
  intersects a group, plus optional `RegisterFragReuse`.
- Without stable row IDs: `RewriteIndex` for each affected index.

TODO: table, one row per action. Action list is still in flux — keep this
table as the source of truth and update §5/§6 from it.

TODO: still to enumerate — Append-only, AddIndex, DropIndex, UpdateConfig,
Restore, ReserveFragments, Project, UpdateBases, Clone, DataReplacement,
Merge.

## 5. Conflict Matrix

N×N table over the action catalog in §4. Each cell:

- **C** — Compatible (commutes, no rebase needed)
- **X** — Conflict (cannot be rebased; fail)
- **R(n)** — Conditional, see rule *n* in §5

TODO: fill matrix once §4 stabilizes.

The intersection rule for criterion-based mask paths (§3.1): a criterion
path (e.g. "all indices over field X") overlaps any concrete path that
satisfies the criterion. This lets a column-update action and a concurrent
AddIndex on that column correctly hit the slow path.

## 6. Conflict Rules

One subsection per `R(n)` cell. Each rule states:

- Detection predicate over payload fields only
- Resolution: rebase rewrite, fail, or auto-merge

TODO.

## 7. Action Lifecycle

Each action has three phases. The split exists to gate IO to one phase
and keep the others pure.

```rust
pub trait Action {
    fn reads(&self) -> ManifestMask;
    fn writes(&self) -> ManifestMask;

    /// Pure. Is this action well-formed against `manifest`?
    /// Catches structural problems: missing fragments, schema mismatch,
    /// invalid field IDs. Not concerned with concurrency.
    fn validate(&self, manifest: &Manifest) -> Result<(), TransactionError>;

    /// Mutates self to absorb concurrent changes. May do IO.
    /// Only called when the mask screen indicates overlap.
    async fn rebase(
        &mut self,
        current: &Manifest,
        concurrent: &[Box<dyn Action>],
    ) -> Result<(), TransactionError>;

    /// Pure. Apply changes; validates inline; no IO.
    fn apply(
        &self,
        manifest: &mut Manifest,
        ctx: &mut TxnContext,
    ) -> Result<ActionOutput, TransactionError>;
}
```

### What each phase owns

| Phase | Pure? | IO? | Question it answers |
|-------|-------|-----|---------------------|
| `validate` | yes | no | Is this action well-formed against this manifest? |
| `rebase`   | mutates self | yes | Can I absorb the changes that landed since base? |
| `apply`    | mutates manifest | no | Mechanical write-through. |

`validate` is decoupled from concurrency. Calling it against base catches
writer bugs early; calling it against latest catches both writer bugs and
concurrency-induced invalidity. The **mask** is the only thing that
answers "did concurrency invalidate me?" cleanly — if the mask says no
overlap and validate fails, it's a writer bug.

### Single-action rebase

Most "rebase" work is already absorbed by §3. Late-bound outputs (§3.2)
need no rewriting; `apply` allocates from current counters. Criterion-
based targeting (§3.1) re-evaluates against the current manifest
automatically.

What remains for `rebase` to actually do: payload fields naming row
address ranges (e.g. `UpdateDeletionVector.affected_rows`,
`ReplaceFragmentColumns.updated_row_offsets`). Row addresses are not
stable across compaction. Conservative rebasing — failing closed when
ranges may have shifted — is acceptable as a starting point.

### Transaction driver

A transaction is an ordered list of actions. The driver mutates a
working clone of the manifest one action at a time:

```rust
async fn commit(
    actions: &[Box<dyn Action>],
    latest: &Manifest,
    intervening_mask: &ManifestMask,
    intervening_actions: &[Box<dyn Action>],
) -> Result<Manifest, TransactionError> {
    let mut working = latest.clone();
    let mut ctx = TxnContext::default();

    for action in actions {
        let touches = action.reads().union(&action.writes());
        if intervening_mask.intersects(&touches) {
            action.rebase(&working, intervening_actions).await?;
        }
        let output = action.apply(&mut working, &mut ctx)?;
        ctx.record(output);
    }
    Ok(working)
}
```

Three consequences worth naming:

1. **Per-action rebase**, not per-transaction. Each action sees the same
   concurrent delta and reasons about it independently.

2. **Per-action validate runs against the in-progress manifest.** Action
   *j*'s `apply` (which validates inline) sees the effects of actions
   *1..j-1* from the same transaction. This matters for Overwrite-style
   transactions where a later `AddFragments` is valid only after an
   earlier `ChangeSchema` lands. See §9.

3. **Atomicity is free.** Mutations are on the in-memory `working`
   clone, which is dropped on failure. No on-disk state is touched until
   the commit protocol (out of scope here) writes the final manifest.

### Symbolic references resolve through `TxnContext`

Per §3.3, intra-txn references are `FragmentRef::ProducedBy { action_idx,
output_idx }`. The `TxnContext` accumulates each action's output as the
driver walks the list, so later actions can resolve them at `apply` time.

TODO: pin down the runtime representation of `FragmentRef` and how it
flows through the serialized form.

## 8. Required Action Payload Changes

Per action, what fields are needed for §6 detection predicates:

- Fragment IDs touched (read / write)
- Field IDs read / written
- Row address ranges (with caveat from §6 on stability)
- Index UUIDs / coverage sets
- MemWAL generation, config keys, etc.

Call out any proto-breaking changes.

TODO.

## 9. Worked Examples

- Append || Append
- Append || Delete
- Update || Update — same row vs. different rows
- Update || Compact
- Delete || Compact
- AddIndex || AddFragment — compound dependency case
- UpdateConfig || UpdateConfig — same key
- Restore || anything — ordering reset

### Overwrite || Overwrite (compound transactions interacting)

Two clients author against the same base M_0. Overwrite decomposes as:

```
[ RemoveFragments(criterion=All),
  ChangeSchema(new_schema),
  AddFragments(new_fragments) ]
```

A commits first → M_1 (schema=S_A, fragments=[F_A]). B then commits.
B's actions walk the driver loop against M_1:

| Step | Action | Mask vs. A | rebase | apply (validates inline against working) |
|------|--------|-----------|--------|----------|
| 1 | RemoveFragments(All) | overlaps FragmentList | re-resolve "all" against M_1 → `{F_A}` | removes F_A |
| 2 | ChangeSchema(S_B)    | overlaps Schema | no-op (overwrite doesn't read prior) | sets schema = S_B |
| 3 | AddFragments(F_B)    | overlaps NextFragmentId, FragmentList | reassign IDs | append F_B; validate sees schema S_B (from step 2) |

B wins; the final state is `(schema=S_B, fragments=[F_B])`. This matches
today's `check_overwrite_txn` semantics (overwrite is treated as
compatible with most concurrent commits — last writer wins).

**Property highlighted:** step 3's `validate` succeeds because step 2
has already mutated the working manifest. If F_B references field IDs
only present in S_B (and not S_0 or S_A), the action is still valid
because its predecessors in the same transaction supplied them.

**Policy question this raises (see §12):** is silent last-writer-wins
on concurrent Overwrites the right default, or should the driver
refuse to commit B when its base ≠ latest unless the user opted into
"ignore concurrent writes"?

## 10. Edge Cases

- Restore (time travel resets the baseline)
- Schema-altering actions vs. data actions
- Actions that become no-ops after rebase
- MemWAL state transitions
- Replacement-of-replacement chains
- Rebased transaction producing a different observable result than the
  original — when is that acceptable?
- Writer-blind index invalidation — handled by §3.1, called out here so
  readers know it's a deliberate property of the layer.

## 11. Migration from Operation-Level Checks

Walk each existing `check_*_txn` in `conflict_resolver.rs` and show which
action-level rule(s) replace it. Flag any check that cannot be expressed at
action level — either lift the missing information into an action payload
(§8) or document the lost semantics.

TODO. Decide later whether this section ships in this doc or a follow-up.

## 12. Future: Multiple Isolation Levels

Once the matrix is in place, sketch what would change to offer:

- A stricter level (e.g. serializable) — likely more `R`/`X` cells.
- A looser level (e.g. read-committed-ish for high-throughput ingestion).

Just a sketch; not a commitment.

## Open Questions

- Is the action list close enough to stable to freeze §4, or should we
  parameterize the matrix?
- Do we need a stable row-ID concept to make any §6 rules non-conservative,
  or is conservative-fail acceptable indefinitely?
- Does §11 (migration map) belong in this doc?
