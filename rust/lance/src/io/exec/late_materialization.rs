// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Late-materialization physical optimizer rule.
//!
//! Defers reading data columns that a row-reducing operator only carries
//! through, fetching them by `_rowaddr` after the row count has shrunk. Used by
//! `merge_insert` to avoid scanning wide non-source columns for every target
//! row of a selective partial-schema upsert.

use std::collections::HashSet;
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    error::Result as DFResult,
    logical_expr::JoinType,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        ExecutionPlan,
        joins::HashJoinExec,
        projection::{ProjectionExec, ProjectionExpr},
    },
};
use datafusion_physical_expr::{PhysicalExpr, PhysicalExprRef, expressions::Column};
use lance_arrow::DataTypeExt;
use lance_core::datatypes::OnMissing;
use lance_core::{ROW_ADDR, ROW_ID};

use super::TakeExec;
use super::filtered_read::FilteredReadExec;

/// Rewrite every [`Column`] in `expr` to reference `schema` by name. Used to
/// re-index a projection's expressions after the column layout of its input
/// changed (e.g. a column moved because it is now sourced from a [`TakeExec`]).
fn reindex_columns_by_name(
    expr: Arc<dyn PhysicalExpr>,
    schema: &ArrowSchema,
) -> DFResult<Arc<dyn PhysicalExpr>> {
    Ok(expr
        .transform_down(|e| {
            if let Some(col) = e.as_any().downcast_ref::<Column>() {
                let new_col = Column::new_with_schema(col.name(), schema)?;
                Ok(Transformed::yes(Arc::new(new_col) as Arc<dyn PhysicalExpr>))
            } else {
                Ok(Transformed::no(e))
            }
        })?
        .data)
}

/// Width/storage gate mirroring the scanner's late-materialization heuristic
/// ([`crate::dataset::scanner::MaterializationStyle::Heuristic`]): a column is
/// worth deferring only if it is "wide" for the backing storage — a
/// variable-width type (strings, lists, vectors) or a fixed-width type above the
/// per-row byte threshold (1KB on cloud storage, 10 bytes on local). Narrow
/// columns are cheaper to read in the sequential scan than to re-fetch by
/// address.
///
/// Without a join-cardinality estimate (tracked in #4583) we cannot gate on
/// match selectivity, so we fall back to width alone. This covers the
/// inherently selective backfill case the feature targets; a follow-up can
/// incorporate cardinality once it is available.
pub fn is_wide_column(field: &lance_core::datatypes::Field, is_cloud: bool) -> bool {
    if field.is_blob() {
        return false;
    }
    let byte_width = field.data_type().byte_width_opt();
    if is_cloud {
        byte_width.is_none_or(|bw| bw >= 1000)
    } else {
        byte_width.is_none_or(|bw| bw >= 10)
    }
}

/// Late-materialization rule: defer reading data columns that a row-reducing
/// operator (here, a [`HashJoinExec`]) only passes through, fetching them by
/// `_rowaddr` *after* the row count has been reduced.
///
/// Concretely, for a `ProjectionExec -> HashJoinExec` where the join's build
/// (left) side is a [`FilteredReadExec`] that emits `_rowaddr`, any data column
/// the scan reads but the join only carries through (not a join key, not used
/// in a join filter) is dropped from the scan, re-fetched by a [`TakeExec`]
/// inserted above the join, and the parent projection is re-indexed to read it
/// from there. The projection's *output* schema is unchanged, so nothing above
/// it is affected.
///
/// This is written generically but is currently applied only at the
/// merge_insert call site (not registered in the session-wide optimizer), which
/// bounds its blast radius. A column missing from the source of a partial-schema
/// upsert is exactly such a "carried-through" column, so deferring it avoids
/// scanning wide columns for every target row when only a few rows match.
#[derive(Debug, Default)]
pub struct LateMaterializeOverReducingJoin;

impl LateMaterializeOverReducingJoin {
    /// Attempt the rewrite for a `ProjectionExec` sitting directly above a
    /// `HashJoinExec`. Returns the rewritten projection, or `None` if the
    /// pattern does not apply or cannot be safely transformed.
    fn try_defer(proj: &ProjectionExec) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(join) = proj.input().as_any().downcast_ref::<HashJoinExec>() else {
            return Ok(None);
        };

        // Only column-preserving join types have a (left ++ right) intermediate
        // schema, which the index remapping below relies on.
        if !matches!(
            join.join_type(),
            JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
        ) {
            return Ok(None);
        }

        // A join filter may reference the columns we want to defer; bail rather
        // than reason about its intermediate schema.
        if join.filter().is_some() {
            return Ok(None);
        }

        // We only handle deferral on the build (left) side, which is where the
        // scanned target relation sits in a merge_insert plan. Deferring on the
        // probe side would need a symmetric (and untested) index remapping.
        let Some(scan) = join.left().as_any().downcast_ref::<FilteredReadExec>() else {
            return Ok(None);
        };

        let left_schema = join.left().schema();
        let right_schema = join.right().schema();
        let left_field_count = left_schema.fields().len();
        let right_field_count = right_schema.fields().len();

        // The scan must emit `_rowaddr` so the deferred columns remain
        // fetchable by address after the join.
        if left_schema.column_with_name(ROW_ADDR).is_none() {
            return Ok(None);
        }

        // Join keys on the build side must stay in the scan.
        let mut left_key_names = HashSet::new();
        for (left, _) in join.on() {
            let Some(col) = left.as_any().downcast_ref::<Column>() else {
                return Ok(None);
            };
            left_key_names.insert(col.name().to_string());
        }

        let dataset = scan.dataset();
        let is_cloud = dataset.object_store.is_cloud();

        // Candidates = scan-side data columns that aren't join keys (and aren't
        // the system `_rowid`/`_rowaddr` columns). These are only used above the
        // join, so they could be fetched after the row count shrinks.
        let candidate_names = left_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .filter(|name| {
                name != ROW_ADDR
                    && name != ROW_ID
                    && !left_key_names.contains(name)
                    && dataset.schema().field(name).is_some()
            })
            .collect::<Vec<_>>();
        if candidate_names.is_empty() {
            return Ok(None);
        }

        // Width/storage gate: only defer columns wide enough that re-fetching
        // by address beats scanning them for every target row.
        let deferred_names = candidate_names
            .iter()
            .filter(|name| {
                dataset
                    .schema()
                    .field(name)
                    .is_some_and(|f| is_wide_column(f, is_cloud))
            })
            .cloned()
            .collect::<Vec<_>>();
        if deferred_names.is_empty() {
            tracing::debug!(
                candidates = ?candidate_names,
                is_cloud,
                "merge_insert late-materialization skipped: no candidate column is wide enough to defer",
            );
            return Ok(None);
        }
        let deferred_set = deferred_names.iter().cloned().collect::<HashSet<_>>();

        // Map each old intermediate (left ++ right) column index to its index
        // after the deferred columns are dropped from the left side.
        let deferred_left_indices = left_schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(i, f)| deferred_set.contains(f.name()).then_some(i))
            .collect::<HashSet<_>>();
        let mut old_to_new = vec![None; left_field_count + right_field_count];
        let mut new_left_len = 0;
        for (i, slot) in old_to_new.iter_mut().enumerate().take(left_field_count) {
            if !deferred_left_indices.contains(&i) {
                *slot = Some(new_left_len);
                new_left_len += 1;
            }
        }
        for j in 0..right_field_count {
            old_to_new[left_field_count + j] = Some(new_left_len + j);
        }

        // Narrow the scan: drop the deferred columns, keep `_rowaddr`.
        let mut narrowed = scan.options().projection.clone();
        narrowed = narrowed.subtract_predicate(|f| deferred_set.contains(&f.name));
        narrowed = narrowed.with_row_addr();
        let new_scan = Arc::new(FilteredReadExec::try_new(
            dataset.clone(),
            scan.options().clone().with_projection(narrowed),
            scan.index_input().cloned(),
        )?) as Arc<dyn ExecutionPlan>;

        // Rebuild the join with the narrowed left child: re-index the keys and
        // the join's output projection, dropping the deferred columns.
        let new_on = join
            .on()
            .iter()
            .map(|(left, right)| {
                let col = left.as_any().downcast_ref::<Column>().unwrap();
                let new_idx = old_to_new[col.index()].expect("join key must not be deferred");
                (
                    Arc::new(Column::new(col.name(), new_idx)) as PhysicalExprRef,
                    right.clone(),
                )
            })
            .collect::<Vec<_>>();
        let new_join_projection = join.projection.as_ref().map(|p| {
            p.iter()
                .filter_map(|&idx| old_to_new[idx])
                .collect::<Vec<_>>()
        });
        let new_join = HashJoinExec::try_new(
            new_scan,
            join.right().clone(),
            new_on,
            None,
            join.join_type(),
            new_join_projection,
            *join.partition_mode(),
            join.null_equality(),
            join.null_aware,
        )?;

        // Defensive: `_rowaddr` must survive into the take's input.
        let join_schema = new_join.schema();
        if join_schema.column_with_name(ROW_ADDR).is_none() {
            return Ok(None);
        }
        // If the join emits duplicate column names — e.g. both the left and
        // right join keys survive because the join has no output projection —
        // we can neither build the take's (lance) schema nor re-index the
        // parent projection by name. Leave such plans untransformed.
        let mut seen = HashSet::with_capacity(join_schema.fields().len());
        if join_schema.fields().iter().any(|f| !seen.insert(f.name())) {
            return Ok(None);
        }
        let join_input = Arc::new(new_join) as Arc<dyn ExecutionPlan>;

        // Insert the take that re-fetches the deferred columns by `_rowaddr`.
        // For an outer join, unmatched (insert) rows have a null `_rowaddr` and
        // must yield NULL deferred values, so the taken fields are nullable.
        let mut take_projection = dataset.empty_projection();
        for name in &deferred_names {
            take_projection = take_projection.union_column(name, OnMissing::Error)?;
        }
        let scan_side_null_extended = matches!(join.join_type(), JoinType::Right | JoinType::Full);
        let take = if scan_side_null_extended {
            TakeExec::try_new_nullable_extra(dataset.clone(), join_input, take_projection)?
        } else {
            TakeExec::try_new(dataset.clone(), join_input, take_projection)?
        };
        let Some(take) = take else {
            return Ok(None);
        };
        let take = Arc::new(take) as Arc<dyn ExecutionPlan>;

        // Re-index the parent projection's expressions onto the take output.
        // Post-join column names are unique, so name-based reindexing is safe.
        let take_schema = take.schema();
        let new_exprs = proj
            .expr()
            .iter()
            .map(|pe| {
                Ok(ProjectionExpr {
                    expr: reindex_columns_by_name(pe.expr.clone(), take_schema.as_ref())?,
                    alias: pe.alias.clone(),
                })
            })
            .collect::<DFResult<Vec<_>>>()?;
        let new_proj = ProjectionExec::try_new(new_exprs, take)?;
        Ok(Some(Arc::new(new_proj)))
    }
}

impl PhysicalOptimizerRule for LateMaterializeOverReducingJoin {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(plan
            .transform_down(|plan| {
                if let Some(proj) = plan.as_any().downcast_ref::<ProjectionExec>()
                    && let Some(rewritten) = Self::try_defer(proj)?
                {
                    return Ok(Transformed::yes(rewritten));
                }
                Ok(Transformed::no(plan))
            })?
            .data)
    }

    fn name(&self) -> &str {
        "late_materialize_over_reducing_join"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
