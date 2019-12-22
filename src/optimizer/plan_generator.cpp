#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "common/exception.h"
#include "optimizer/operator_expression.h"
#include "optimizer/plan_generator.h"
#include "optimizer/properties.h"
#include "optimizer/property_set.h"
#include "optimizer/util.h"
#include "parser/expression_util.h"
#include "settings/settings_manager.h"
#include "transaction/transaction_context.h"

#include "planner/plannodes/aggregate_plan_node.h"
#include "planner/plannodes/csv_scan_plan_node.h"
#include "planner/plannodes/delete_plan_node.h"
#include "planner/plannodes/export_external_file_plan_node.h"
#include "planner/plannodes/hash_join_plan_node.h"
#include "planner/plannodes/index_scan_plan_node.h"
#include "planner/plannodes/insert_plan_node.h"
#include "planner/plannodes/limit_plan_node.h"
#include "planner/plannodes/nested_loop_join_plan_node.h"
#include "planner/plannodes/order_by_plan_node.h"
#include "planner/plannodes/projection_plan_node.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "planner/plannodes/update_plan_node.h"

namespace terrier::optimizer {

PlanGenerator::PlanGenerator() = default;

std::unique_ptr<planner::AbstractPlanNode> PlanGenerator::ConvertOpExpression(
    transaction::TransactionContext *txn, catalog::CatalogAccessor *accessor, settings::SettingsManager *settings,
    OperatorExpression *op, PropertySet *required_props,
    const std::vector<common::ManagedPointer<parser::AbstractExpression>> &required_cols,
    const std::vector<common::ManagedPointer<parser::AbstractExpression>> &output_cols,
    std::vector<std::unique_ptr<planner::AbstractPlanNode>> &&children_plans,
    std::vector<ExprMap> &&children_expr_map) {
  required_props_ = required_props;
  required_cols_ = required_cols;
  output_cols_ = output_cols;
  children_plans_ = std::move(children_plans);
  children_expr_map_ = children_expr_map;
  settings_ = settings;
  accessor_ = accessor;
  txn_ = txn;

  op->GetOp().Accept(common::ManagedPointer<OperatorVisitor>(this));

  CorrectOutputPlanWithProjection();
  return std::move(output_plan_);
}

void PlanGenerator::CorrectOutputPlanWithProjection() {
  if (output_cols_ == required_cols_) {
    // We have the correct columns, so no projection needed
    return;
  }

  std::vector<ExprMap> output_expr_maps = {ExprMap{}};
  auto &child_expr_map = output_expr_maps[0];

  // child_expr_map is a map from output_cols_ -> index
  // where the index is the column location in tuple (like value_idxx)
  for (size_t idx = 0; idx < output_cols_.size(); ++idx) {
    auto &col = output_cols_[idx];
    child_expr_map[col] = static_cast<unsigned int>(idx);
  }

  std::vector<planner::OutputSchema::Column> columns;
  for (auto &col : required_cols_) {
    col->DeriveReturnValueType();
    if (child_expr_map.find(col) != child_expr_map.end()) {
      // remapping so point to correct location
      auto dve = std::make_unique<parser::DerivedValueExpression>(col->GetReturnValueType(), 0, child_expr_map[col]);
      columns.emplace_back(col->GetExpressionName(), col->GetReturnValueType(), std::move(dve));
    } else {
      // Evaluate the expression and add to target list
      auto conv_col = parser::ExpressionUtil::ConvertExprCVNodes(col, {child_expr_map});
      auto final_col =
          parser::ExpressionUtil::EvaluateExpression(output_expr_maps, common::ManagedPointer(conv_col.get()));
      columns.emplace_back(col->GetExpressionName(), col->GetReturnValueType(), std::move(final_col));
    }
  }

  // We don't actually want shared_ptr but pending another PR
  auto schema = std::make_unique<planner::OutputSchema>(std::move(columns));

  auto builder = planner::ProjectionPlanNode::Builder();
  builder.SetOutputSchema(std::move(schema));
  if (output_plan_ != nullptr) {
    builder.AddChild(std::move(output_plan_));
  }

  output_plan_ = builder.Build();
}

///////////////////////////////////////////////////////////////////////////////
// TableFreeScan
///////////////////////////////////////////////////////////////////////////////

void PlanGenerator::Visit(UNUSED_ATTRIBUTE const TableFreeScan *op) {
  // TableFreeScan is used in case of SELECT without FROM so that enforcer
  // can enforce a PhysicalProjection on top of TableFreeScan to generate correct
  // result. But here, no need to translate TableFreeScan to any physical plan.
  output_plan_ = nullptr;
}

///////////////////////////////////////////////////////////////////////////////
// SeqScan + IndexScan
///////////////////////////////////////////////////////////////////////////////

// Generate columns for scan plan
std::vector<catalog::col_oid_t> PlanGenerator::GenerateColumnsForScan() {
  std::vector<catalog::col_oid_t> column_ids;
  for (auto &output_expr : output_cols_) {
    TERRIER_ASSERT(output_expr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE,
                   "Scan columns should all be base table columns");

    auto tve = output_expr.CastManagedPointerTo<parser::ColumnValueExpression>();
    auto col_id = tve->GetColumnOid();

    TERRIER_ASSERT(col_id != catalog::INVALID_COLUMN_OID, "TVE should be base");
    column_ids.push_back(col_id);
  }

  return column_ids;
}

std::unique_ptr<planner::OutputSchema> PlanGenerator::GenerateScanOutputSchema(catalog::table_oid_t tbl_oid) {
  // Underlying tuple provided by the table's schema.
  const auto &schema = accessor_->GetSchema(tbl_oid);
  std::unordered_map<catalog::col_oid_t, unsigned> coloid_to_offset;
  for (size_t idx = 0; idx < schema.GetColumns().size(); idx++) {
    coloid_to_offset[schema.GetColumns()[idx].Oid()] = static_cast<unsigned>(idx);
  }

  std::vector<planner::OutputSchema::Column> columns;
  for (auto &output_expr : output_cols_) {
    TERRIER_ASSERT(output_expr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE,
                   "Scan columns should all be base table columns");

    auto tve = output_expr.CastManagedPointerTo<parser::ColumnValueExpression>();
    auto col_id = tve->GetColumnOid();
    auto old_idx = coloid_to_offset.at(col_id);

    // output schema of a plan node is literally the columns of the table.
    // there is no such thing as an intermediate column here!
    auto dve = std::make_unique<parser::DerivedValueExpression>(tve->GetReturnValueType(), 0, old_idx);
    columns.emplace_back(tve->GetColumnName(), tve->GetReturnValueType(), std::move(dve));
  }

  return std::make_unique<planner::OutputSchema>(std::move(columns));
}

std::unique_ptr<parser::AbstractExpression> PlanGenerator::GeneratePredicateForScan(
    common::ManagedPointer<parser::AbstractExpression> predicate_expr, const std::string &alias,
    catalog::db_oid_t db_oid, catalog::table_oid_t tbl_oid) {
  if (predicate_expr.Get() == nullptr) {
    return nullptr;
  }

  ExprMap table_expr_map;
  auto exprs = OptimizerUtil::GenerateTableColumnValueExprs(accessor_, alias, db_oid, tbl_oid);
  for (size_t idx = 0; idx < exprs.size(); idx++) {
    table_expr_map[common::ManagedPointer(exprs[idx])] = static_cast<int>(idx);
  }

  // Makes a copy
  auto pred = parser::ExpressionUtil::EvaluateExpression({table_expr_map}, predicate_expr);
  for (auto *expr : exprs) {
    delete expr;
  }
  return pred;
}

void PlanGenerator::Visit(const SeqScan *op) {
  // Generate output column IDs for plan
  std::vector<catalog::col_oid_t> column_ids = GenerateColumnsForScan();

  // Generate the predicate in the scan
  auto conj_expr = parser::ExpressionUtil::JoinAnnotatedExprs(op->GetPredicates());
  auto predicate = GeneratePredicateForScan(common::ManagedPointer(conj_expr), op->GetTableAlias(),
                                            op->GetDatabaseOID(), op->GetTableOID())
                       .release();
  RegisterPointerCleanup<parser::AbstractExpression>(predicate, true, true);

  // OutputSchema
  auto output_schema = GenerateScanOutputSchema(op->GetTableOID());

  // Build
  output_plan_ = planner::SeqScanPlanNode::Builder()
                     .SetOutputSchema(std::move(output_schema))
                     .SetDatabaseOid(op->GetDatabaseOID())
                     .SetNamespaceOid(op->GetNamespaceOID())
                     .SetTableOid(op->GetTableOID())
                     .SetScanPredicate(common::ManagedPointer(predicate))
                     .SetColumnOids(std::move(column_ids))
                     .SetIsForUpdateFlag(op->GetIsForUpdate())
                     .Build();
}

void PlanGenerator::Visit(const IndexScan *op) {
  // Generate ouptut column IDs for plan
  // An IndexScan (for now at least) will output all columns of its table
  std::vector<catalog::col_oid_t> column_ids = GenerateColumnsForScan();

  catalog::table_oid_t tbl_oid = accessor_->GetTableOid(op->GetNamespaceOID(), op->GetTableAlias());
  auto output_schema = GenerateScanOutputSchema(tbl_oid);

  // Generate the predicate in the scan
  auto conj_expr = parser::ExpressionUtil::JoinAnnotatedExprs(op->GetPredicates());
  auto predicate =
      GeneratePredicateForScan(common::ManagedPointer(conj_expr), op->GetTableAlias(), op->GetDatabaseOID(), tbl_oid)
          .release();
  RegisterPointerCleanup<parser::AbstractExpression>(predicate, true, true);

  // Create index scan desc
  // Can destructively move from physical operator since we won't need them anymore
  auto tuple_oids = std::vector<catalog::col_oid_t>(op->GetKeyColumnOIDList());
  auto expr_list = std::vector<parser::ExpressionType>(op->GetExprTypeList());

  // We do a copy since const_cast is not good
  std::vector<type::TransientValue> value_list;
  for (auto &value : op->GetValueList()) {
    auto val_copy = type::TransientValue(value);
    value_list.push_back(std::move(val_copy));
  }

  auto index_desc = planner::IndexScanDescription(std::move(tuple_oids), std::move(expr_list), std::move(value_list));

  output_plan_ = planner::IndexScanPlanNode::Builder()
                     .SetOutputSchema(std::move(output_schema))
                     .SetScanPredicate(common::ManagedPointer(predicate))
                     .SetIsForUpdateFlag(op->GetIsForUpdate())
                     .SetDatabaseOid(op->GetDatabaseOID())
                     .SetNamespaceOid(op->GetNamespaceOID())
                     .SetIndexOid(op->GetIndexOID())
                     .SetColumnOids(std::move(column_ids))
                     .SetIndexScanDescription(std::move(index_desc))
                     .Build();
}

void PlanGenerator::Visit(const ExternalFileScan *op) {
  switch (op->GetFormat()) {
    case parser::ExternalFileFormat::CSV: {
      // First construct the output column descriptions
      std::vector<type::TypeId> value_types;
      std::vector<planner::OutputSchema::Column> cols;

      auto idx = 0;
      for (auto output_col : output_cols_) {
        auto dve = std::make_unique<parser::DerivedValueExpression>(output_col->GetReturnValueType(), 0, idx);
        cols.emplace_back(output_col->GetExpressionName(), output_col->GetReturnValueType(), std::move(dve));
        value_types.push_back(output_col->GetReturnValueType());
        idx++;
      }

      auto output_schema = std::make_unique<planner::OutputSchema>(std::move(cols));
      output_plan_ = planner::CSVScanPlanNode::Builder()
                         .SetOutputSchema(std::move(output_schema))
                         .SetFileName(op->GetFilename())
                         .SetDelimiter(op->GetDelimiter())
                         .SetQuote(op->GetQuote())
                         .SetEscape(op->GetEscape())
                         .SetValueTypes(std::move(value_types))
                         .Build();
      break;
    }
    case parser::ExternalFileFormat::BINARY: {
      TERRIER_ASSERT(0, "Missing BinaryScanPlanNode");
    }
  }
}

void PlanGenerator::Visit(const QueryDerivedScan *op) {
  // QueryDerivedScan is a renaming layer...
  // This can be satisfied using a projection to correct the OutputSchema
  TERRIER_ASSERT(children_plans_.size() == 1, "QueryDerivedScan must have 1 children plan");
  auto &child_expr_map = children_expr_map_[0];
  auto alias_expr_map = op->GetAliasToExprMap();

  std::vector<planner::OutputSchema::Column> columns;
  for (auto &output : output_cols_) {
    // We can assert this based on InputColumnDeriver::Visit(const QueryDerivedScan *op)
    auto colve = output.CastManagedPointerTo<parser::ColumnValueExpression>();

    // Get offset into child_expr_ma
    auto expr = alias_expr_map.at(colve->GetColumnName());
    auto offset = child_expr_map.at(expr);

    auto dve = std::make_unique<parser::DerivedValueExpression>(colve->GetReturnValueType(), 0, offset);
    columns.emplace_back(colve->GetColumnName(), colve->GetReturnValueType(), std::move(dve));
  }

  auto schema = std::make_unique<planner::OutputSchema>(std::move(columns));
  output_plan_ = planner::ProjectionPlanNode::Builder()
                     .SetOutputSchema(std::move(schema))
                     .AddChild(std::move(children_plans_[0]))
                     .Build();
}

///////////////////////////////////////////////////////////////////////////////
// Limit + Sort
///////////////////////////////////////////////////////////////////////////////

void PlanGenerator::Visit(const Limit *op) {
  // Generate order by + limit plan when there's internal sort order
  // Limit and sort have the same output schema as the child plan!
  TERRIER_ASSERT(children_plans_.size() == 1, "Limit needs 1 child plan");
  output_plan_ = std::move(children_plans_[0]);

  if (!op->GetSortExpressions().empty()) {
    // Build order by clause
    TERRIER_ASSERT(children_expr_map_.size() == 1, "Limit needs 1 child expr map");
    auto &child_cols_map = children_expr_map_[0];

    TERRIER_ASSERT(op->GetSortExpressions().size() == op->GetSortAscending().size(),
                   "Limit sort expressions and sort ascending size should match");

    // OrderBy OutputSchema does not add/drop columns. All output columns of OrderBy
    // are the same as the output columns of the child plan. As such, the OutputSchema
    // of an OrderBy has the same columns vector as the child OutputSchema, with only
    // DerivedValueExpressions
    auto idx = 0;
    auto &child_plan_cols = output_plan_->GetOutputSchema()->GetColumns();
    std::vector<planner::OutputSchema::Column> child_columns;
    for (auto &col : child_plan_cols) {
      auto dve = std::make_unique<parser::DerivedValueExpression>(col.GetType(), 0, idx);
      child_columns.emplace_back(col.GetName(), col.GetType(), std::move(dve));
      idx++;
    }
    auto output_schema = std::make_unique<planner::OutputSchema>(std::move(child_columns));
    auto order_build = planner::OrderByPlanNode::Builder();
    order_build.SetOutputSchema(std::move(output_schema));
    order_build.AddChild(std::move(output_plan_));
    order_build.SetLimit(op->GetLimit());
    order_build.SetOffset(op->GetOffset());

    auto &sort_columns = op->GetSortExpressions();
    auto &sort_flags = op->GetSortAscending();
    auto sort_column_size = sort_columns.size();
    for (size_t idx = 0; idx < sort_column_size; idx++) {
      // Based on InputColumnDeriver, sort columns should be provided

      // Evaluate the sort_column using children_expr_map (what the child provides)
      // Need to replace ColumnValueExpression with DerivedValueExpression
      auto eval_expr = parser::ExpressionUtil::EvaluateExpression({child_cols_map}, sort_columns[idx]).release();
      RegisterPointerCleanup<parser::AbstractExpression>(eval_expr, true, true);
      order_build.AddSortKey(common::ManagedPointer(eval_expr), sort_flags[idx]);
    }

    output_plan_ = order_build.Build();
  }

  // Limit OutputSchema does not add/drop columns. All output columns of Limit
  // are the same as the output columns of the child plan. As such, the OutputSchema
  // of an Limit has the same columns vector as the child OutputSchema, with only
  // DerivedValueExpressions
  auto idx = 0;
  auto &child_plan_cols = output_plan_->GetOutputSchema()->GetColumns();
  std::vector<planner::OutputSchema::Column> child_columns;
  for (auto &col : child_plan_cols) {
    auto dve = std::make_unique<parser::DerivedValueExpression>(col.GetType(), 0, idx);
    child_columns.emplace_back(col.GetName(), col.GetType(), std::move(dve));
    idx++;
  }

  auto limit_out = std::make_unique<planner::OutputSchema>(std::move(child_columns));
  output_plan_ = planner::LimitPlanNode::Builder()
                     .SetOutputSchema(std::move(limit_out))
                     .SetLimit(op->GetLimit())
                     .SetOffset(op->GetOffset())
                     .AddChild(std::move(output_plan_))
                     .Build();
}

void PlanGenerator::Visit(UNUSED_ATTRIBUTE const OrderBy *op) {
  // OrderBy reorders tuples - should keep the same output schema as the original child plan
  TERRIER_ASSERT(children_plans_.size() == 1, "OrderBy needs 1 child plan");
  TERRIER_ASSERT(children_expr_map_.size() == 1, "OrderBy needs 1 child expr map");
  auto &child_cols_map = children_expr_map_[0];

  auto sort_prop = required_props_->GetPropertyOfType(PropertyType::SORT)->As<PropertySort>();
  TERRIER_ASSERT(sort_prop != nullptr, "OrderBy requires a sort property");

  auto sort_columns_size = sort_prop->GetSortColumnSize();

  auto builder = planner::OrderByPlanNode::Builder();

  // OrderBy OutputSchema does not add/drop columns. All output columns of OrderBy
  // are the same as the output columns of the child plan. As such, the OutputSchema
  // of an OrderBy has the same columns vector as the child OutputSchema, with only
  // DerivedValueExpressions
  auto idx = 0;
  auto &child_plan_cols = children_plans_[0]->GetOutputSchema()->GetColumns();
  std::vector<planner::OutputSchema::Column> child_columns;
  for (auto &col : child_plan_cols) {
    auto dve = std::make_unique<parser::DerivedValueExpression>(col.GetType(), 0, idx);
    child_columns.emplace_back(col.GetName(), col.GetType(), std::move(dve));
    idx++;
  }
  builder.SetOutputSchema(std::make_unique<planner::OutputSchema>(std::move(child_columns)));

  for (size_t i = 0; i < sort_columns_size; ++i) {
    auto sort_dir = sort_prop->GetSortAscending(static_cast<int>(i));
    auto key = sort_prop->GetSortColumn(i);

    // Evaluate the sort_column using children_expr_map (what the child provides)
    // Need to replace ColumnValueExpression with DerivedValueExpression
    auto eval_expr = parser::ExpressionUtil::EvaluateExpression({child_cols_map}, key).release();
    RegisterPointerCleanup<parser::AbstractExpression>(eval_expr, true, true);
    builder.AddSortKey(common::ManagedPointer(eval_expr), sort_dir);
  }

  builder.AddChild(std::move(children_plans_[0]));
  output_plan_ = builder.Build();
}

///////////////////////////////////////////////////////////////////////////////
// ExportExternalFile
///////////////////////////////////////////////////////////////////////////////

void PlanGenerator::Visit(const ExportExternalFile *op) {
  TERRIER_ASSERT(children_plans_.size() == 1, "ExportExternalFile needs 1 child");

  output_plan_ = planner::ExportExternalFilePlanNode::Builder()
                     .AddChild(std::move(children_plans_[0]))
                     .SetFileName(op->GetFilename())
                     .SetDelimiter(op->GetDelimiter())
                     .SetQuote(op->GetQuote())
                     .SetEscape(op->GetEscape())
                     .SetFileFormat(op->GetFormat())
                     .Build();
}

///////////////////////////////////////////////////////////////////////////////
// Join Projection Helper
///////////////////////////////////////////////////////////////////////////////

std::unique_ptr<planner::OutputSchema> PlanGenerator::GenerateProjectionForJoin() {
  TERRIER_ASSERT(children_expr_map_.size() == 2, "Join needs 2 children");
  TERRIER_ASSERT(children_plans_.size() == 2, "Join needs 2 children");
  auto &l_child_expr_map = children_expr_map_[0];
  auto &r_child_expr_map = children_expr_map_[1];

  std::vector<planner::OutputSchema::Column> columns;
  for (auto &expr : output_cols_) {
    auto type = expr->GetReturnValueType();
    if (l_child_expr_map.count(expr) != 0U) {
      auto dve = std::make_unique<parser::DerivedValueExpression>(type, 0, l_child_expr_map[expr]);
      columns.emplace_back(expr->GetExpressionName(), type, std::move(dve));
    } else if (r_child_expr_map.count(expr) != 0U) {
      auto dve = std::make_unique<parser::DerivedValueExpression>(type, 1, r_child_expr_map[expr]);
      columns.emplace_back(expr->GetExpressionName(), type, std::move(dve));
    } else {
      auto eval = parser::ExpressionUtil::EvaluateExpression(children_expr_map_, expr);
      columns.emplace_back(expr->GetExpressionName(), type, std::move(eval));
    }
  }

  return std::make_unique<planner::OutputSchema>(std::move(columns));
}

///////////////////////////////////////////////////////////////////////////////
// A NLJoin B (the join to not do on large relations)
///////////////////////////////////////////////////////////////////////////////

void PlanGenerator::Visit(const InnerNLJoin *op) {
  auto proj_schema = GenerateProjectionForJoin();

  auto comb_pred = parser::ExpressionUtil::JoinAnnotatedExprs(op->GetJoinPredicates());
  auto eval_pred = parser::ExpressionUtil::EvaluateExpression(children_expr_map_, common::ManagedPointer(comb_pred));
  auto join_predicate =
      parser::ExpressionUtil::ConvertExprCVNodes(common::ManagedPointer(eval_pred), children_expr_map_).release();
  RegisterPointerCleanup<parser::AbstractExpression>(join_predicate, true, true);

  std::vector<common::ManagedPointer<parser::AbstractExpression>> left_keys;
  std::vector<common::ManagedPointer<parser::AbstractExpression>> right_keys;
  for (auto &expr : op->GetLeftKeys()) {
    auto left_key = parser::ExpressionUtil::EvaluateExpression({children_expr_map_[0]}, expr).release();
    RegisterPointerCleanup<parser::AbstractExpression>(left_key, true, true);
    left_keys.emplace_back(left_key);
  }

  for (auto &expr : op->GetRightKeys()) {
    auto right_key = parser::ExpressionUtil::EvaluateExpression({children_expr_map_[1]}, expr).release();
    RegisterPointerCleanup<parser::AbstractExpression>(right_key, true, true);
    right_keys.emplace_back(right_key);
  }

  output_plan_ = planner::NestedLoopJoinPlanNode::Builder()
                     .SetOutputSchema(std::move(proj_schema))
                     .SetJoinPredicate(common::ManagedPointer(join_predicate))
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetLeftKeys(std::move(left_keys))
                     .SetRightKeys(std::move(right_keys))
                     .AddChild(std::move(children_plans_[0]))
                     .AddChild(std::move(children_plans_[1]))
                     .Build();
}

void PlanGenerator::Visit(UNUSED_ATTRIBUTE const LeftNLJoin *op) { TERRIER_ASSERT(0, "LeftNLJoin not implemented"); }

void PlanGenerator::Visit(UNUSED_ATTRIBUTE const RightNLJoin *op) { TERRIER_ASSERT(0, "RightNLJoin not implemented"); }

void PlanGenerator::Visit(UNUSED_ATTRIBUTE const OuterNLJoin *op) { TERRIER_ASSERT(0, "OuterNLJoin not implemented"); }

///////////////////////////////////////////////////////////////////////////////
// A hashjoin B (what you should do for large relations.....)
///////////////////////////////////////////////////////////////////////////////

void PlanGenerator::Visit(const InnerHashJoin *op) {
  auto proj_schema = GenerateProjectionForJoin();

  auto comb_pred = parser::ExpressionUtil::JoinAnnotatedExprs(op->GetJoinPredicates());
  auto eval_pred =
      parser::ExpressionUtil::EvaluateExpression(children_expr_map_, common::ManagedPointer(comb_pred.get()));
  auto join_predicate =
      parser::ExpressionUtil::ConvertExprCVNodes(common::ManagedPointer(eval_pred.get()), children_expr_map_).release();
  RegisterPointerCleanup<parser::AbstractExpression>(join_predicate, true, true);

  auto builder = planner::HashJoinPlanNode::Builder();
  builder.SetOutputSchema(std::move(proj_schema));

  std::vector<ExprMap> l_child_map{std::move(children_expr_map_[0])};
  std::vector<ExprMap> r_child_map{std::move(children_expr_map_[1])};
  for (auto &expr : op->GetLeftKeys()) {
    auto left_key = parser::ExpressionUtil::EvaluateExpression(l_child_map, expr).release();
    RegisterPointerCleanup<parser::AbstractExpression>(left_key, true, true);
    builder.AddLeftHashKey(common::ManagedPointer(left_key));
  }

  for (auto &expr : op->GetRightKeys()) {
    auto right_key = parser::ExpressionUtil::EvaluateExpression(r_child_map, expr).release();
    RegisterPointerCleanup<parser::AbstractExpression>(right_key, true, true);
    builder.AddRightHashKey(common::ManagedPointer(right_key));
  }

  builder.AddChild(std::move(children_plans_[0]));
  builder.AddChild(std::move(children_plans_[1]));
  output_plan_ = builder.Build();
}

void PlanGenerator::Visit(UNUSED_ATTRIBUTE const LeftHashJoin *op) {
  TERRIER_ASSERT(0, "LeftHashJoin not implemented");
}

void PlanGenerator::Visit(UNUSED_ATTRIBUTE const RightHashJoin *op) {
  TERRIER_ASSERT(0, "RightHashJoin not implemented");
}

void PlanGenerator::Visit(UNUSED_ATTRIBUTE const OuterHashJoin *op) {
  TERRIER_ASSERT(0, "OuterHashJoin not implemented");
}

///////////////////////////////////////////////////////////////////////////////
// Aggregations (when the groups are greater than individuals)
///////////////////////////////////////////////////////////////////////////////

void PlanGenerator::BuildAggregatePlan(
    planner::AggregateStrategyType aggr_type,
    const std::vector<common::ManagedPointer<parser::AbstractExpression>> *groupby_cols,
    common::ManagedPointer<parser::AbstractExpression> having_predicate) {
  TERRIER_ASSERT(children_expr_map_.size() == 1, "Aggregate needs 1 child plan");
  auto &child_expr_map = children_expr_map_[0];

  auto builder = planner::AggregatePlanNode::Builder();

  auto agg_id = 0;
  ExprMap output_expr_map;
  std::vector<planner::OutputSchema::Column> columns;
  for (size_t idx = 0; idx < output_cols_.size(); ++idx) {
    auto expr = output_cols_[idx];
    expr->DeriveReturnValueType();
    output_expr_map[expr] = static_cast<unsigned>(idx);

    if (parser::ExpressionUtil::IsAggregateExpression(expr->GetExpressionType())) {
      // We need to evaluate the expression first, convert ColumnValue => DerivedValue
      auto eval = parser::ExpressionUtil::EvaluateExpression(children_expr_map_, expr).release();
      TERRIER_ASSERT(parser::ExpressionUtil::IsAggregateExpression(eval->GetExpressionType()),
                     "Evaluated AggregateExpression should still be an aggregate expression");

      auto agg_expr = reinterpret_cast<parser::AggregateExpression *>(eval);
      RegisterPointerCleanup<parser::AggregateExpression>(agg_expr, true, true);
      builder.AddAggregateTerm(common::ManagedPointer(agg_expr));

      // Maps the aggregate value in the right tuple to the output
      // See aggregateor.cpp for more detail...
      // TODO([Execution Engine]): make sure this behavior still is correct
      auto dve = std::make_unique<parser::DerivedValueExpression>(expr->GetReturnValueType(), 1, agg_id++);
      columns.emplace_back(expr->GetExpressionName(), expr->GetReturnValueType(), std::move(dve));
    } else if (child_expr_map.find(expr) != child_expr_map.end()) {
      auto dve = std::make_unique<parser::DerivedValueExpression>(expr->GetReturnValueType(), 0, child_expr_map[expr]);
      columns.emplace_back(expr->GetExpressionName(), expr->GetReturnValueType(), std::move(dve));
    } else {
      auto eval = parser::ExpressionUtil::EvaluateExpression(children_expr_map_, expr);
      columns.emplace_back(expr->GetExpressionName(), expr->GetReturnValueType(), std::move(eval));
    }
  }

  // Generate group by ids
  if (groupby_cols != nullptr) {
    for (auto &col : *groupby_cols) {
      auto eval = parser::ExpressionUtil::EvaluateExpression({child_expr_map}, col);
      auto gb_term =
          parser::ExpressionUtil::ConvertExprCVNodes(common::ManagedPointer(eval), {child_expr_map}).release();
      RegisterPointerCleanup<parser::AbstractExpression>(gb_term, true, true);
      builder.AddGroupByTerm(common::ManagedPointer(gb_term));
    }
  }

  // Generate the output schema
  auto output_schema = std::make_unique<planner::OutputSchema>(std::move(columns));

  // Prepare having clause
  auto eval_have = parser::ExpressionUtil::EvaluateExpression({output_expr_map}, having_predicate);
  auto predicate =
      parser::ExpressionUtil::ConvertExprCVNodes(common::ManagedPointer(eval_have), {output_expr_map}).release();
  RegisterPointerCleanup<parser::AbstractExpression>(predicate, true, true);

  builder.SetOutputSchema(std::move(output_schema));
  builder.SetHavingClausePredicate(common::ManagedPointer(predicate));
  builder.SetAggregateStrategyType(aggr_type);
  builder.AddChild(std::move(children_plans_[0]));
  output_plan_ = builder.Build();
}

void PlanGenerator::Visit(const HashGroupBy *op) {
  auto having_predicates = parser::ExpressionUtil::JoinAnnotatedExprs(op->GetHaving());
  BuildAggregatePlan(planner::AggregateStrategyType::HASH, &op->GetColumns(),
                     common::ManagedPointer(having_predicates.get()));
}

void PlanGenerator::Visit(const SortGroupBy *op) {
  // Don't think this is ever visited since rule_impls.cpp does not create SortGroupBy
  auto having_predicates = parser::ExpressionUtil::JoinAnnotatedExprs(op->GetHaving());
  BuildAggregatePlan(planner::AggregateStrategyType::SORTED, &op->GetColumns(),
                     common::ManagedPointer(having_predicates.get()));
}

void PlanGenerator::Visit(UNUSED_ATTRIBUTE const Aggregate *op) {
  BuildAggregatePlan(planner::AggregateStrategyType::PLAIN, nullptr, nullptr);
}

///////////////////////////////////////////////////////////////////////////////
// Insert/Update/Delete
// To update or delete or select tuples, one must insert them first
///////////////////////////////////////////////////////////////////////////////

void PlanGenerator::Visit(const Insert *op) {
  auto builder = planner::InsertPlanNode::Builder();
  builder.SetDatabaseOid(op->GetDatabaseOid());
  builder.SetNamespaceOid(op->GetNamespaceOid());
  builder.SetTableOid(op->GetTableOid());

  std::vector<catalog::index_oid_t> indexes(op->GetIndexes());
  builder.SetIndexOids(std::move(indexes));

  auto values = op->GetValues();
  for (auto &tuple_value : values) {
    builder.AddValues(std::move(tuple_value));
  }

  // This is based on what Peloton does/did with query_to_operator_transformer.cpp
  TERRIER_ASSERT(!op->GetColumns().empty(), "Transformer should added columns");
  for (auto &col : op->GetColumns()) {
    builder.AddParameterInfo(col);
  }

  // Schema of Insert is empty
  builder.SetOutputSchema(std::make_unique<planner::OutputSchema>());
  output_plan_ = builder.Build();
}

void PlanGenerator::Visit(const InsertSelect *op) {
  // Schema of Insert is empty
  TERRIER_ASSERT(children_plans_.size() == 1, "InsertSelect needs 1 child plan");
  auto output_schema = std::make_unique<planner::OutputSchema>();

  output_plan_ = planner::InsertPlanNode::Builder()
                     .SetOutputSchema(std::move(output_schema))
                     .SetDatabaseOid(op->GetDatabaseOid())
                     .SetNamespaceOid(op->GetNamespaceOid())
                     .SetTableOid(op->GetTableOid())
                     .AddChild(std::move(children_plans_[0]))
                     .Build();
}

void PlanGenerator::Visit(const Delete *op) {
  // OutputSchema of DELETE is empty vector
  TERRIER_ASSERT(children_plans_.size() == 1, "Delete should have 1 child plan");
  auto output_schema = std::make_unique<planner::OutputSchema>();

  output_plan_ = planner::DeletePlanNode::Builder()
                     .SetOutputSchema(std::move(output_schema))
                     .SetDatabaseOid(op->GetDatabaseOid())
                     .SetNamespaceOid(op->GetNamespaceOid())
                     .SetTableOid(op->GetTableOid())
                     .AddChild(std::move(children_plans_[0]))
                     .Build();
}

void PlanGenerator::Visit(const Update *op) {
  auto builder = planner::UpdatePlanNode::Builder();
  std::unordered_set<catalog::col_oid_t> update_col_offsets;

  const auto &alias = op->GetTableAlias();
  auto tbl_oid = op->GetTableOid();
  auto tbl_schema = accessor_->GetSchema(tbl_oid);

  ExprMap table_expr_map;
  auto exprs = OptimizerUtil::GenerateTableColumnValueExprs(accessor_, alias, op->GetDatabaseOid(), tbl_oid);
  for (size_t idx = 0; idx < exprs.size(); idx++) {
    table_expr_map[common::ManagedPointer(exprs[idx])] = static_cast<int>(idx);
  }

  // Evaluate update expression and add to target list
  auto updates = op->GetUpdateClauses();
  for (auto &update : updates) {
    auto col = tbl_schema.GetColumn(update->GetColumnName());
    auto col_id = col.Oid();
    if (update_col_offsets.find(col_id) != update_col_offsets.end())
      throw SYNTAX_EXCEPTION("Multiple assignments to same column");

    update_col_offsets.insert(col_id);
    auto value = parser::ExpressionUtil::EvaluateExpression({table_expr_map}, update->GetUpdateValue()).release();
    RegisterPointerCleanup<parser::AbstractExpression>(value, true, true);
    builder.AddSetClause(std::make_pair(col_id, common::ManagedPointer<parser::AbstractExpression>(value)));
  }

  for (auto expr : exprs) {
    delete expr;
  }

  // Empty OutputSchema for update
  auto output_schema = std::make_unique<planner::OutputSchema>();

  // TODO(wz2): What is this SetUpdatePrimaryKey
  output_plan_ = builder.SetOutputSchema(std::move(output_schema))
                     .SetDatabaseOid(op->GetDatabaseOid())
                     .SetNamespaceOid(op->GetNamespaceOid())
                     .SetTableOid(op->GetTableOid())
                     .SetUpdatePrimaryKey(false)
                     .AddChild(std::move(children_plans_[0]))
                     .Build();
}

}  // namespace terrier::optimizer
