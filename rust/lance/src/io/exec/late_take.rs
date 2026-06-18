// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Late-materialization *logical* optimizer rule.
//!
//! Defers reading wide data columns that a row-reducing join only carries
//! through, fetching them by `_rowaddr` *after* the row count has shrunk. Used
//! by `merge_insert` to avoid scanning wide non-source columns for every target
//! row of a selective partial-schema upsert.
//!
//! Working at the logical level (rather than rewriting a physical
//! `HashJoinExec` by position), a [`LateTakeNode`] is inserted above the join —
//! fed by a projection that keeps only the columns carried past the join — and
//! advertises an output schema of "carried columns plus the deferred columns
//! appended". Its [`UserDefinedLogicalNodeCore::necessary_children_exprs`]
//! reports that it does *not* need the deferred columns from its child, only
//! `_rowaddr`. DataFusion's stock `OptimizeProjections` rule then prunes those
//! columns from the scan automatically — no manual index remapping — and
//! downstream column references resolve the deferred columns from the take by
//! name.
//!
//! The node lowers to the existing physical [`super::TakeExec`] via
//! [`LateTakePlanner`].

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use arrow_schema::{Field as ArrowField, Schema as ArrowSchema};
use async_trait::async_trait;
use datafusion::{
    common::{
        Column, DFSchema, DFSchemaRef, Result as DFResult, TableReference,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    },
    datasource::DefaultTableSource,
    execution::SessionState,
    logical_expr::{Expr, Extension, Join, JoinType, LogicalPlan, Projection},
    optimizer::{OptimizerConfig, OptimizerRule},
    physical_plan::ExecutionPlan,
    physical_planner::{ExtensionPlanner, PhysicalPlanner},
};
use datafusion_expr::{UserDefinedLogicalNode, UserDefinedLogicalNodeCore};
use lance_arrow::DataTypeExt;
use lance_core::datatypes::OnMissing;
use lance_core::{ROW_ADDR, ROW_ID};

use super::TakeExec;
use crate::Dataset;
use crate::datafusion::dataframe::LanceTableProvider;

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
fn is_wide_column(field: &lance_core::datatypes::Field, is_cloud: bool) -> bool {
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

/// Logical plan node that re-fetches `deferred_columns` from `dataset` by
/// `_rowaddr` after a row-reducing operator.
///
/// Output schema = the input's columns with `deferred_columns` removed,
/// followed by the deferred columns appended in dataset-schema order (mirroring
/// the physical [`TakeExec`], which appends taken columns). Constructing the
/// schema this way makes it invariant under projection pushdown: whether or not
/// the child still produces a deferred column, the node advertises the same
/// output, so the rule can be inserted before pushdown prunes the scan.
#[derive(Debug)]
pub struct LateTakeNode {
    input: LogicalPlan,
    dataset: Arc<Dataset>,
    /// Dataset field names to re-fetch by address, in dataset-schema order.
    deferred_columns: Vec<String>,
    /// Qualifier for the appended deferred fields (e.g. `target`), matching the
    /// relation the scanned columns came from.
    qualifier: Option<TableReference>,
    /// When true the deferred fields are nullable in the output even if the
    /// dataset declares them non-null. Set above an outer join where the scan
    /// side can be null-extended (its `_rowaddr` is null → NULL deferred value).
    nullable_extra: bool,
    schema: DFSchemaRef,
}

impl PartialEq for LateTakeNode {
    fn eq(&self, other: &Self) -> bool {
        self.dataset.base == other.dataset.base
            && self.deferred_columns == other.deferred_columns
            && self.qualifier == other.qualifier
            && self.nullable_extra == other.nullable_extra
            && self.input == other.input
    }
}

impl Eq for LateTakeNode {}

impl std::hash::Hash for LateTakeNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.dataset.base.hash(state);
        self.deferred_columns.hash(state);
        self.qualifier.hash(state);
        self.nullable_extra.hash(state);
        self.input.hash(state);
    }
}

impl PartialOrd for LateTakeNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.deferred_columns.partial_cmp(&other.deferred_columns) {
            Some(std::cmp::Ordering::Equal) => self.input.partial_cmp(&other.input),
            cmp => cmp,
        }
    }
}

impl LateTakeNode {
    pub fn try_new(
        input: LogicalPlan,
        dataset: Arc<Dataset>,
        deferred_columns: Vec<String>,
        qualifier: Option<TableReference>,
        nullable_extra: bool,
    ) -> DFResult<Self> {
        let schema = Self::build_output_schema(
            &input,
            &dataset,
            &deferred_columns,
            &qualifier,
            nullable_extra,
        )?;
        Ok(Self {
            input,
            dataset,
            deferred_columns,
            qualifier,
            nullable_extra,
            schema,
        })
    }

    /// Build `input columns (minus deferred) ++ deferred columns appended`.
    fn build_output_schema(
        input: &LogicalPlan,
        dataset: &Dataset,
        deferred_columns: &[String],
        qualifier: &Option<TableReference>,
        nullable_extra: bool,
    ) -> DFResult<DFSchemaRef> {
        let input_schema = input.schema();
        let deferred_set: HashSet<&str> = deferred_columns.iter().map(|s| s.as_str()).collect();

        let mut qualified_fields: Vec<(Option<TableReference>, Arc<ArrowField>)> = input_schema
            .iter()
            .filter(|(_, f)| !deferred_set.contains(f.name().as_str()))
            .map(|(q, f)| (q.cloned(), f.clone()))
            .collect();

        let dataset_arrow = ArrowSchema::from(dataset.schema());
        for name in deferred_columns {
            let field = dataset_arrow.field_with_name(name).map_err(|e| {
                datafusion::error::DataFusionError::Plan(format!(
                    "late-materialization: deferred column '{name}' not found in dataset schema: {e}"
                ))
            })?;
            let field = if nullable_extra && !field.is_nullable() {
                field.clone().with_nullable(true)
            } else {
                field.clone()
            };
            qualified_fields.push((qualifier.clone(), Arc::new(field)));
        }

        Ok(Arc::new(DFSchema::new_with_metadata(
            qualified_fields,
            input_schema.metadata().clone(),
        )?))
    }

    /// Index of the row-address (or, failing that, row-id) column in the child.
    fn row_locator_index(&self) -> Option<usize> {
        let input_schema = self.input.schema();
        input_schema
            .index_of_column_by_name(self.qualifier.as_ref(), ROW_ADDR)
            .or_else(|| input_schema.index_of_column_by_name(self.qualifier.as_ref(), ROW_ID))
    }
}

impl UserDefinedLogicalNodeCore for LateTakeNode {
    fn name(&self) -> &str {
        "LateTake"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "LateTake: deferred=[{}], nullable_extra={}",
            self.deferred_columns.join(", "),
            self.nullable_extra
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        if !exprs.is_empty() {
            return Err(datafusion::error::DataFusionError::Internal(
                "LateTakeNode does not accept expressions".to_string(),
            ));
        }
        if inputs.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(
                "LateTakeNode requires exactly one input".to_string(),
            ));
        }
        Self::try_new(
            inputs.remove(0),
            self.dataset.clone(),
            self.deferred_columns.clone(),
            self.qualifier.clone(),
            self.nullable_extra,
        )
    }

    /// Drive projection pushdown: the deferred columns are produced by this
    /// node (fetched by address), so they are never requested from the child;
    /// `_rowaddr` is always required so the fetch remains possible.
    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        let input_schema = self.input.schema();
        let deferred_set: HashSet<&str> =
            self.deferred_columns.iter().map(|s| s.as_str()).collect();

        // Output positions [0..passthrough_len) map back to these child indices,
        // in order; positions beyond are the appended (fetched) deferred columns.
        let passthrough: Vec<usize> = input_schema
            .iter()
            .enumerate()
            .filter(|(_, (_, f))| !deferred_set.contains(f.name().as_str()))
            .map(|(i, _)| i)
            .collect();

        let row_locator = self.row_locator_index()?;

        let mut needed = BTreeSet::new();
        for &oc in output_columns {
            if let Some(child_idx) = passthrough.get(oc) {
                needed.insert(*child_idx);
            }
        }
        needed.insert(row_locator);
        Some(vec![needed.into_iter().collect()])
    }
}

/// Logical optimizer rule that inserts a [`LateTakeNode`] above a join when a
/// wide column from a Lance table relation is only carried through the join.
///
/// Detection is qualifier-driven, not side-specific: it inspects both join
/// inputs for a [`LanceTableProvider`] scan that emits `_rowaddr`, so it keeps
/// working if build/probe sides are swapped. The actual scan narrowing is left
/// to `OptimizeProjections`, which must run after this rule.
#[derive(Debug, Default)]
pub struct LateMaterializeJoin;

impl LateMaterializeJoin {
    pub fn new() -> Self {
        Self
    }

    /// Recover the Lance dataset backing a join input, descending only through
    /// single-input nodes (alias/projection/filter) so a nested join's scan is
    /// never picked up by mistake.
    fn find_lance_dataset(plan: &LogicalPlan) -> Option<Arc<Dataset>> {
        if let LogicalPlan::TableScan(scan) = plan {
            let source = scan.source.as_any().downcast_ref::<DefaultTableSource>()?;
            let provider = source
                .table_provider
                .as_any()
                .downcast_ref::<LanceTableProvider>()?;
            return Some(provider.dataset());
        }
        let inputs = plan.inputs();
        if inputs.len() == 1 {
            Self::find_lance_dataset(inputs[0])
        } else {
            None
        }
    }

    /// Names of the join's equi-keys on `side` (the columns that must stay in
    /// the scan because the join reads them).
    fn join_key_names(join: &Join, side: JoinSide) -> HashSet<String> {
        join.on
            .iter()
            .filter_map(|(left, right)| {
                let expr = match side {
                    JoinSide::Left => left,
                    JoinSide::Right => right,
                };
                match expr {
                    Expr::Column(col) => Some(col.name.clone()),
                    _ => None,
                }
            })
            .collect()
    }

    /// Collect every `(qualifier, name)` column reference that appears in an
    /// expression anywhere in `plan`. Used to tell which scan-side columns are
    /// actually consumed *above* a join (and so worth re-fetching) rather than
    /// merely produced by the scan.
    fn collect_referenced_columns(
        plan: &LogicalPlan,
    ) -> DFResult<HashSet<(Option<TableReference>, String)>> {
        let mut referenced = HashSet::new();
        plan.apply(|node| {
            // A join's own on-clause / filter columns are consumed by the join
            // itself, not by an operator above it, so they must not count as
            // "used above the join". (Equi-keys are the common case: both sides'
            // keys share a name, which would otherwise look like a duplicate
            // column flowing into the take.) Children are still visited.
            if matches!(node, LogicalPlan::Join(_)) {
                return Ok(TreeNodeRecursion::Continue);
            }
            for expr in node.expressions() {
                expr.apply(|e| {
                    if let Expr::Column(col) = e {
                        referenced.insert((col.relation.clone(), col.name.clone()));
                    }
                    Ok(TreeNodeRecursion::Continue)
                })?;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(referenced)
    }

    fn try_defer_join(
        join: &Join,
        referenced: &HashSet<(Option<TableReference>, String)>,
    ) -> DFResult<Option<LogicalPlan>> {
        // Only column-preserving joins; a join filter may reference a column we
        // would defer, so bail rather than reason about it.
        if !matches!(
            join.join_type,
            JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
        ) || join.filter.is_some()
        {
            return Ok(None);
        }

        for side in [JoinSide::Left, JoinSide::Right] {
            let side_plan: &LogicalPlan = match side {
                JoinSide::Left => &join.left,
                JoinSide::Right => &join.right,
            };
            let Some(dataset) = Self::find_lance_dataset(side_plan) else {
                continue;
            };
            let side_schema = side_plan.schema();

            // The scan side must emit `_rowaddr` so deferred columns stay
            // fetchable by address after the join.
            if side_schema
                .index_of_column_by_name(None, ROW_ADDR)
                .is_none()
            {
                continue;
            }
            let qualifier = side_schema
                .iter()
                .find(|(_, f)| f.name() == ROW_ADDR)
                .and_then(|(q, _)| q.cloned());

            let key_names = Self::join_key_names(join, side);
            let is_cloud = dataset.object_store.is_cloud();
            let dataset_arrow = ArrowSchema::from(dataset.schema());

            // Candidates = scan-side data columns that aren't join keys nor the
            // system columns, that are consumed above the join, and are wide
            // enough to be worth re-fetching by address.
            let mut deferred: Vec<(usize, String)> = Vec::new();
            for (col_qualifier, field) in side_schema.iter() {
                let name = field.name();
                if name == ROW_ADDR || name == ROW_ID || key_names.contains(name) {
                    continue;
                }
                // Only defer columns actually used above the join; a column the
                // scan produces but nobody references would just be pruned, so
                // re-fetching it would be wasted work.
                if !referenced.contains(&(col_qualifier.cloned(), name.clone())) {
                    continue;
                }
                let Some(ds_field) = dataset.schema().field(name) else {
                    continue;
                };
                if !is_wide_column(ds_field, is_cloud) {
                    continue;
                }
                let Ok(ds_idx) = dataset_arrow.index_of(name) else {
                    continue;
                };
                deferred.push((ds_idx, name.clone()));
            }
            if deferred.is_empty() {
                continue;
            }
            // Append in dataset-schema order to match TakeExec's output order.
            deferred.sort_by_key(|(idx, _)| *idx);
            let deferred_columns: Vec<String> =
                deferred.into_iter().map(|(_, name)| name).collect();

            // Insert a normalizing projection between the join and the take that
            // keeps only the columns the take must carry: those referenced above
            // the join plus the row locator, with the deferred columns dropped
            // (they are re-fetched). This serves two purposes:
            //   * It lets the physical planner give the join a tight output
            //     projection. Without it, the opaque take node forces the
            //     `HashJoinExec` to emit its full `left ++ right` output, which
            //     for an equi-join includes both sides' key columns — duplicate
            //     arrow names that `TakeExec` (which merges by name) cannot
            //     handle once qualifiers are erased at the physical level.
            //   * It is where scan narrowing happens: the deferred columns are
            //     simply absent from the projection, so projection pushdown drops
            //     them from the scan while keeping `_rowaddr`.
            // `referenced` excludes the join's own on-clause columns, so a
            // redundant equi-key (used only by the join) is pruned here rather
            // than colliding with the surviving key. If two *referenced* columns
            // still share a name, the take cannot disambiguate them; bail.
            let deferred_set: HashSet<&str> = deferred_columns.iter().map(|s| s.as_str()).collect();
            let mut kept_exprs: Vec<Expr> = Vec::new();
            // Seed with the deferred names: they are appended to the take output,
            // so a kept column sharing one of those names also conflicts.
            let mut kept_names: HashSet<String> = deferred_columns.iter().cloned().collect();
            let mut name_conflict = false;
            for (col_qualifier, field) in join.schema.iter() {
                let name = field.name();
                // Drop the deferred columns: they are re-fetched and appended by
                // the take, not carried through its input.
                if col_qualifier == qualifier.as_ref() && deferred_set.contains(name.as_str()) {
                    continue;
                }
                let kept = name == ROW_ADDR
                    || name == ROW_ID
                    || referenced.contains(&(col_qualifier.cloned(), name.clone()));
                if !kept {
                    continue;
                }
                if !kept_names.insert(name.clone()) {
                    name_conflict = true;
                    break;
                }
                kept_exprs.push(Expr::Column(Column::new(col_qualifier.cloned(), name)));
            }
            if name_conflict {
                continue;
            }

            let take_input = LogicalPlan::Projection(Projection::try_new(
                kept_exprs,
                Arc::new(LogicalPlan::Join(join.clone())),
            )?);

            // The scan side is null-extended when it is the optional side of an
            // outer join; its unmatched rows then have a null `_rowaddr` and
            // must yield NULL deferred values.
            let nullable_extra = matches!(
                (side, join.join_type),
                (JoinSide::Left, JoinType::Right)
                    | (JoinSide::Left, JoinType::Full)
                    | (JoinSide::Right, JoinType::Left)
                    | (JoinSide::Right, JoinType::Full)
            );

            let node = LateTakeNode::try_new(
                take_input,
                dataset,
                deferred_columns,
                qualifier,
                nullable_extra,
            )?;
            return Ok(Some(LogicalPlan::Extension(Extension {
                node: Arc::new(node),
            })));
        }

        Ok(None)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinSide {
    Left,
    Right,
}

impl OptimizerRule for LateMaterializeJoin {
    fn name(&self) -> &str {
        "late_materialize_join"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        let referenced = Self::collect_referenced_columns(&plan)?;
        plan.transform_down(|node| {
            // Never descend into an already-deferred subtree, otherwise the
            // inner join would be wrapped again on this or a later pass.
            if let LogicalPlan::Extension(ext) = &node
                && ext.node.as_any().is::<LateTakeNode>()
            {
                return Ok(Transformed::new(node, false, TreeNodeRecursion::Jump));
            }
            if let LogicalPlan::Join(join) = &node
                && let Some(wrapped) = Self::try_defer_join(join, &referenced)?
            {
                // Jump: the freshly wrapped join must not be revisited.
                return Ok(Transformed::new(wrapped, true, TreeNodeRecursion::Jump));
            }
            Ok(Transformed::no(node))
        })
    }
}

/// Lowers a [`LateTakeNode`] to the physical [`TakeExec`].
#[derive(Debug)]
pub struct LateTakePlanner;

#[async_trait]
impl ExtensionPlanner for LateTakePlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(take_node) = node.as_any().downcast_ref::<LateTakeNode>() else {
            return Ok(None);
        };
        assert_eq!(physical_inputs.len(), 1, "LateTake requires one input");
        let input = physical_inputs[0].clone();

        let mut projection = take_node.dataset.empty_projection();
        for name in &take_node.deferred_columns {
            projection = projection.union_column(name, OnMissing::Error)?;
        }

        let take = if take_node.nullable_extra {
            TakeExec::try_new_nullable_extra(take_node.dataset.clone(), input.clone(), projection)?
        } else {
            TakeExec::try_new(take_node.dataset.clone(), input.clone(), projection)?
        };

        // `try_new` returns None when no extra columns are needed; fall back to
        // the raw input so the plan still lowers.
        Ok(Some(match take {
            Some(take) => Arc::new(take) as Arc<dyn ExecutionPlan>,
            None => input,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::{Array, Int32Array, RecordBatch, RecordBatchIterator, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef};
    use datafusion::execution::{SessionStateBuilder, TaskContext};
    use datafusion::logical_expr::TableScan;
    use datafusion::optimizer::{
        Optimizer, OptimizerContext, optimize_projections::OptimizeProjections,
    };
    use datafusion::physical_plan::{collect, displayable};
    use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
    use datafusion::prelude::*;
    use lance_core::utils::tempfile::TempStrDir;

    use crate::datafusion::dataframe::SessionContextExt;
    use crate::dataset::WriteParams;

    /// Write a `{id: Int32 (key), payload: Utf8 (wide), tag: Int32 (narrow)}`
    /// dataset to a temp dir and open it.
    async fn test_dataset() -> (Arc<Dataset>, TempStrDir) {
        let schema: SchemaRef = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("payload", DataType::Utf8, true),
            Field::new("tag", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec![
                    "alpha", "bravo", "charlie", "delta", "echo",
                ])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap();

        let tmp = TempStrDir::default();
        let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema.clone());
        Dataset::write(reader, tmp.as_str(), Some(WriteParams::default()))
            .await
            .unwrap();
        let dataset = Arc::new(Dataset::open(tmp.as_str()).await.unwrap());
        (dataset, tmp)
    }

    /// A source DataFrame `{sid}` aliased "source". A distinct key name (vs the
    /// target's `id`) keeps the join output free of duplicate names so the take
    /// can sit above it.
    fn source_df(ctx: &SessionContext, ids: Vec<i32>) -> DataFrame {
        let schema: SchemaRef = Arc::new(ArrowSchema::new(vec![Field::new(
            "sid",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids))]).unwrap();
        ctx.read_batch(batch).unwrap().alias("source").unwrap()
    }

    fn find_table_scan(plan: &LogicalPlan) -> Option<&TableScan> {
        if let LogicalPlan::TableScan(scan) = plan {
            return Some(scan);
        }
        plan.inputs().into_iter().find_map(find_table_scan)
    }

    fn has_late_take(plan: &LogicalPlan) -> bool {
        if let LogicalPlan::Extension(ext) = plan
            && ext.node.as_any().is::<LateTakeNode>()
        {
            return true;
        }
        plan.inputs().iter().any(|p| has_late_take(p))
    }

    fn scan_column_names(plan: &LogicalPlan) -> Vec<String> {
        find_table_scan(plan)
            .unwrap()
            .projected_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    fn run_rule_and_pushdown(plan: LogicalPlan) -> LogicalPlan {
        let optimizer = Optimizer::with_rules(vec![
            Arc::new(LateMaterializeJoin::new()),
            Arc::new(OptimizeProjections::new()),
        ]);
        optimizer
            .optimize(plan, &OptimizerContext::new(), |_, _| {})
            .unwrap()
    }

    /// Build `Projection(select cols) <- Join(target, source) <- scan`.
    async fn join_plan(
        ctx: &SessionContext,
        dataset: Arc<Dataset>,
        join_type: JoinType,
        select: &[&str],
        source_ids: Vec<i32>,
    ) -> LogicalPlan {
        let target = ctx
            .read_lance_unordered(dataset, false, true)
            .unwrap()
            .alias("target")
            .unwrap();
        let source = source_df(ctx, source_ids);
        let exprs = select.iter().map(|c| col(*c)).collect::<Vec<_>>();
        target
            .join(source, join_type, &["id"], &["sid"], None)
            .unwrap()
            .select(exprs)
            .unwrap()
            .into_unoptimized_plan()
    }

    #[tokio::test]
    async fn test_wide_column_deferred() {
        let (dataset, _tmp) = test_dataset().await;
        let ctx = SessionContext::new();
        let plan = join_plan(
            &ctx,
            dataset,
            JoinType::Inner,
            &["target.id", "target.payload", "target.tag"],
            vec![1, 3],
        )
        .await;

        let before = plan.schema().clone();
        let optimized = run_rule_and_pushdown(plan);

        assert!(
            has_late_take(&optimized),
            "expected a LateTake node:\n{}",
            optimized.display_indent()
        );

        let scan_cols = scan_column_names(&optimized);
        assert!(scan_cols.contains(&"id".to_string()), "scan: {scan_cols:?}");
        assert!(
            scan_cols.contains(&ROW_ADDR.to_string()),
            "scan: {scan_cols:?}"
        );
        // wide column dropped from the scan; narrow `tag` (used above the join)
        // is not deferred and stays in the scan.
        assert!(
            !scan_cols.contains(&"payload".to_string()),
            "payload should be deferred, scan: {scan_cols:?}"
        );
        assert!(
            scan_cols.contains(&"tag".to_string()),
            "scan: {scan_cols:?}"
        );

        // The plan's output schema is unchanged by the rewrite.
        assert_eq!(before.fields(), optimized.schema().fields());
    }

    #[tokio::test]
    async fn test_no_wide_columns_no_take() {
        let (dataset, _tmp) = test_dataset().await;
        let ctx = SessionContext::new();
        // Only narrow columns used above the join → nothing to defer.
        let plan = join_plan(
            &ctx,
            dataset,
            JoinType::Inner,
            &["target.id", "target.tag"],
            vec![1, 3],
        )
        .await;
        let optimized = run_rule_and_pushdown(plan);

        assert!(!has_late_take(&optimized), "no take expected");
        let scan_cols = scan_column_names(&optimized);
        assert!(
            scan_cols.contains(&"tag".to_string()),
            "scan: {scan_cols:?}"
        );
    }

    #[tokio::test]
    async fn test_join_key_not_deferred() {
        let (dataset, _tmp) = test_dataset().await;
        let ctx = SessionContext::new();
        // Join on the wide `payload` column: as a key it must stay in the scan,
        // and there is no other wide column to defer.
        let target = ctx
            .read_lance_unordered(dataset, false, true)
            .unwrap()
            .alias("target")
            .unwrap();
        let source_schema: SchemaRef = Arc::new(ArrowSchema::new(vec![Field::new(
            "spayload",
            DataType::Utf8,
            true,
        )]));
        let source_batch = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(StringArray::from(vec!["alpha", "charlie"]))],
        )
        .unwrap();
        let source = ctx
            .read_batch(source_batch)
            .unwrap()
            .alias("source")
            .unwrap();
        let plan = target
            .join(source, JoinType::Inner, &["payload"], &["spayload"], None)
            .unwrap()
            .select(vec![col("target.id"), col("target.payload")])
            .unwrap()
            .into_unoptimized_plan();

        let optimized = run_rule_and_pushdown(plan);
        assert!(!has_late_take(&optimized), "join key must not be deferred");
        let scan_cols = scan_column_names(&optimized);
        assert!(
            scan_cols.contains(&"payload".to_string()),
            "join key stays in scan: {scan_cols:?}"
        );
    }

    #[tokio::test]
    async fn test_duplicate_join_output_names_not_deferred() {
        let (dataset, _tmp) = test_dataset().await;
        let ctx = SessionContext::new();
        // Both sides expose `payload` and both are consumed above the join, so
        // the take (which merges appended columns by name) would produce a
        // duplicate `payload` and must not be inserted.
        let target = ctx
            .read_lance_unordered(dataset, false, true)
            .unwrap()
            .alias("target")
            .unwrap();
        let source_schema: SchemaRef = Arc::new(ArrowSchema::new(vec![
            Field::new("sid", DataType::Int32, false),
            Field::new("payload", DataType::Utf8, true),
        ]));
        let source_batch = RecordBatch::try_new(
            source_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 3])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap();
        let source = ctx
            .read_batch(source_batch)
            .unwrap()
            .alias("source")
            .unwrap();
        let plan = target
            .join(source, JoinType::Inner, &["id"], &["sid"], None)
            .unwrap()
            // Reference both sides' `payload` above the join: deferring
            // `target.payload` would collide with the kept `source.payload`.
            .select(vec![
                col("target.id"),
                col("target.payload"),
                col("source.payload"),
            ])
            .unwrap()
            .into_unoptimized_plan();

        let optimized = run_rule_and_pushdown(plan);
        assert!(
            !has_late_take(&optimized),
            "duplicate output names must not be deferred"
        );
    }

    #[tokio::test]
    async fn test_outer_join_marks_deferred_nullable() {
        let (dataset, _tmp) = test_dataset().await;
        let ctx = SessionContext::new();
        // RIGHT join: target (left input) is the null-extended side.
        let plan = join_plan(
            &ctx,
            dataset,
            JoinType::Right,
            &["target.id", "target.payload"],
            vec![1, 3],
        )
        .await;
        let optimized = run_rule_and_pushdown(plan);
        assert!(has_late_take(&optimized));

        // Locate the node and confirm the deferred field is nullable.
        fn find_node(plan: &LogicalPlan) -> Option<&LateTakeNode> {
            if let LogicalPlan::Extension(ext) = plan
                && let Some(n) = ext.node.as_any().downcast_ref::<LateTakeNode>()
            {
                return Some(n);
            }
            plan.inputs().into_iter().find_map(find_node)
        }
        let node = find_node(&optimized).unwrap();
        assert!(node.nullable_extra);
        let payload = UserDefinedLogicalNodeCore::schema(node)
            .field_with_unqualified_name("payload")
            .unwrap();
        assert!(payload.is_nullable());
    }

    #[tokio::test]
    async fn test_necessary_children_exprs() {
        let (dataset, _tmp) = test_dataset().await;
        let ctx = SessionContext::new();
        let input = ctx
            .read_lance_unordered(dataset.clone(), false, true)
            .unwrap()
            .alias("target")
            .unwrap()
            .into_unoptimized_plan();
        // child schema order: id(0), payload(1), tag(2), _rowaddr(3)
        let rowaddr_idx = input
            .schema()
            .index_of_column_by_name(None, ROW_ADDR)
            .unwrap();
        let node = LateTakeNode::try_new(
            input,
            dataset,
            vec!["payload".to_string()],
            Some(TableReference::bare("target")),
            false,
        )
        .unwrap();

        // Output: id(0), tag(1), _rowaddr(2), payload(3, appended/fetched).
        // All outputs requested → child needs id, tag, _rowaddr (not payload).
        assert_eq!(
            UserDefinedLogicalNodeCore::necessary_children_exprs(&node, &[0, 1, 2, 3]),
            Some(vec![vec![0, 2, rowaddr_idx]])
        );
        // Only the deferred column requested → child still only needs _rowaddr.
        assert_eq!(
            UserDefinedLogicalNodeCore::necessary_children_exprs(&node, &[3]),
            Some(vec![vec![rowaddr_idx]])
        );
        // Nothing requested → _rowaddr is still forced in.
        assert_eq!(
            UserDefinedLogicalNodeCore::necessary_children_exprs(&node, &[]),
            Some(vec![vec![rowaddr_idx]])
        );
    }

    /// Lower the rewritten plan to physical and confirm it (a) inserts a
    /// `TakeExec` and (b) produces exactly the same rows as the un-deferred plan,
    /// including NULL deferred values for source-only rows of an outer join.
    async fn assert_execution_parity(join_type: JoinType, source_ids: Vec<i32>) {
        let (dataset, _tmp) = test_dataset().await;
        let ctx = SessionContext::new();
        let state = SessionStateBuilder::new().with_default_features().build();

        let plan = join_plan(
            &ctx,
            dataset,
            join_type,
            &["target.id", "target.payload"],
            source_ids,
        )
        .await;

        // Baseline: standard optimization + default planner.
        let baseline_logical = state.optimize(&plan).unwrap();
        let baseline = state.create_physical_plan(&baseline_logical).await.unwrap();
        let baseline_rows = collect(baseline, Arc::new(TaskContext::default()))
            .await
            .unwrap();

        // Deferred: insert the take, prune, then lower with our planner.
        let optimized = run_rule_and_pushdown(plan);
        let planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(LateTakePlanner)]);
        let physical = planner
            .create_physical_plan(&optimized, &state)
            .await
            .unwrap();
        let rendered = displayable(physical.as_ref()).indent(true).to_string();
        assert!(
            rendered.contains("Take"),
            "expected a TakeExec in the plan:\n{rendered}"
        );
        let deferred_rows = collect(physical, Arc::new(TaskContext::default()))
            .await
            .unwrap();

        // Compare row sets (order-independent).
        let sort = |batches: &[RecordBatch]| {
            let mut rows: Vec<(Option<i32>, Option<String>)> = Vec::new();
            for b in batches {
                let ids = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
                let payloads = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                for i in 0..b.num_rows() {
                    rows.push((
                        (!ids.is_null(i)).then(|| ids.value(i)),
                        (!payloads.is_null(i)).then(|| payloads.value(i).to_string()),
                    ));
                }
            }
            rows.sort();
            rows
        };
        assert_eq!(sort(&baseline_rows), sort(&deferred_rows));
    }

    #[tokio::test]
    async fn test_execution_parity_inner() {
        assert_execution_parity(JoinType::Inner, vec![1, 3]).await;
    }

    #[tokio::test]
    async fn test_execution_parity_outer_null_rowaddr() {
        // RIGHT join with a source-only id (99) that has no target match: that
        // row's `target._rowaddr` is null, so the take must scatter a NULL
        // payload for it — matching the un-deferred plan.
        assert_execution_parity(JoinType::Right, vec![1, 99]).await;
    }
}
