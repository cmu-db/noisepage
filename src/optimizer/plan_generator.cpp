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
#include "parser/expression_util.h"
#include "settings/settings_manager.h"
#include "transaction/transaction_context.h"

#include "planner/plannodes/aggregate_plan_node.h"
#include "planner/plannodes/csv_scan_plan_node.h"
#include "planner/plannodes/delete_plan_node.h"
#include "planner/plannodes/export_external_file_plan_node.h"
#include "planner/plannodes/hash_join_plan_node.h"
#include "planner/plannodes/hash_plan_node.h"
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

std::shared_ptr<planner::AbstractPlanNode> PlanGenerator::ConvertOpExpression(
    OperatorExpression *op, PropertySet *required_props,
    const std::vector<const parser::AbstractExpression *> &required_cols,
    const std::vector<const parser::AbstractExpression *> &output_cols,
    std::vector<std::shared_ptr<planner::AbstractPlanNode>> &&children_plans, std::vector<ExprMap> &&children_expr_map,
    settings::SettingsManager *settings, catalog::CatalogAccessor *accessor, transaction::TransactionContext *txn) {
  required_props_ = required_props;
  required_cols_ = required_cols;
  output_cols_ = output_cols;
  children_plans_ = children_plans;
  children_expr_map_ = children_expr_map;
  settings_ = settings;
  accessor_ = accessor;
  txn_ = txn;

  op->GetOp().Accept(this);

  CorrectOutputPlanWithProjection();
  return output_plan_;
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

  // "intermediate column" => output derived from base plan
  std::vector<planner::OutputSchema::DerivedTarget> tl;

  // "renaming" => output points to column in base plan
  std::vector<planner::OutputSchema::DirectMap> dml;

  std::vector<planner::OutputSchema::Column> columns;
  for (size_t idx = 0; idx < required_cols_.size(); ++idx) {
    auto &col = required_cols_[idx];
    const_cast<parser::AbstractExpression *>(col)->DeriveReturnValueType();
    if (child_expr_map.find(col) != child_expr_map.end()) {
      // remapping so point to correct location
      dml.emplace_back(idx, std::make_pair(0, child_expr_map[col]));
      columns.emplace_back(col->GetExpressionName(), col->GetReturnValueType());
    } else {
      planner::OutputSchema::Column column(col->GetExpressionName(), col->GetReturnValueType());

      // Evaluate the expression and add to target list
      // final_col will be freed by DerivedColumn

      // TODO(boweic) : integrate the following two functions
      auto conv_col = parser::ExpressionUtil::ConvertExprCVNodes(col, {child_expr_map});
      auto final_col = parser::ExpressionUtil::EvaluateExpression(output_expr_maps, conv_col);
      delete conv_col;

      auto *derived = new planner::OutputSchema::DerivedColumn(column, final_col);
      tl.emplace_back(idx, derived);
      columns.push_back(column);
    }
  }

  // We don't actually want shared_ptr but pending another PR
  auto schema = std::make_shared<planner::OutputSchema>(std::move(columns), std::move(tl), std::move(dml));
  auto &builder = planner::ProjectionPlanNode::Builder().SetOutputSchema(std::move(schema));
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

std::vector<const parser::AbstractExpression *> PlanGenerator::GenerateTableColumnValueExprs(
    const std::string &alias, catalog::db_oid_t db_oid, catalog::table_oid_t tbl_oid) {
  // TODO(boweic): we seems to provide all columns here, in case where there are
  // a lot of attributes and we're only visiting a few this is not efficient
  auto &schema = accessor_->GetSchema(tbl_oid);
  auto &columns = schema.GetColumns();
  std::vector<const parser::AbstractExpression *> exprs(columns.size());
  for (auto &column : columns) {
    auto col_oid = column.Oid();
    auto *col_expr = new parser::ColumnValueExpression(alias, column.Name());
    col_expr->SetReturnValueType(column.Type());
    col_expr->SetDatabaseOID(db_oid);
    col_expr->SetTableOID(tbl_oid);
    col_expr->SetColumnOID(col_oid);
    exprs.push_back(col_expr);
  }

  return exprs;
}

// Generate columns for scan plan
std::vector<catalog::col_oid_t> PlanGenerator::GenerateColumnsForScan() {
  std::vector<catalog::col_oid_t> column_ids;
  for (auto &output_expr : output_cols_) {
    TERRIER_ASSERT(output_expr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE,
                   "Scan columns should all be base table columns");

    auto tve = dynamic_cast<const parser::ColumnValueExpression *>(output_expr);
    auto col_id = tve->GetColumnOid();

    TERRIER_ASSERT(col_id != catalog::INVALID_COLUMN_OID, "TVE should be base");
    column_ids.push_back(col_id);
  }

  return column_ids;
}

std::shared_ptr<planner::OutputSchema> PlanGenerator::GenerateScanOutputSchema(catalog::table_oid_t tbl_oid) {
  // Since the GenerateColumnsForScan only includes col_oid of actual scan columns
  // the OutputSchema should only contain dml for those columns.
  auto &schema = accessor_->GetSchema(tbl_oid);
  auto &columns = schema.GetColumns();
  std::unordered_map<catalog::col_oid_t, unsigned> coloid_to_offset;
  for (size_t idx = 0; idx < columns.size(); idx++) {
    coloid_to_offset[columns[idx].Oid()] = static_cast<unsigned>(idx);
  }

  std::vector<planner::OutputSchema::DerivedTarget> tl;
  std::vector<planner::OutputSchema::DirectMap> dml;
  std::vector<planner::OutputSchema::Column> output_schema_columns;
  auto idx = 0;
  for (auto &output_expr : output_cols_) {
    TERRIER_ASSERT(output_expr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE,
                   "Scan columns should all be base table columns");

    auto tve = dynamic_cast<const parser::ColumnValueExpression *>(output_expr);
    auto col_id = tve->GetColumnOid();
    auto old_idx = coloid_to_offset.at(col_id);

    // output schema of a plan node is literally the columns of the table.
    // there is no such thing as an intermediate column here!
    dml.emplace_back(static_cast<uint32_t>(idx), std::make_pair(0, static_cast<uint32_t>(old_idx)));
    output_schema_columns.emplace_back(tve->GetExpressionName(), tve->GetReturnValueType());
    idx++;
  }

  return std::make_shared<planner::OutputSchema>(std::move(output_schema_columns), std::move(tl), std::move(dml));
}

const parser::AbstractExpression *PlanGenerator::GeneratePredicateForScan(
    const parser::AbstractExpression *predicate_expr, const std::string &alias, catalog::db_oid_t db_oid,
    catalog::table_oid_t tbl_oid) {
  if (predicate_expr == nullptr) {
    return nullptr;
  }

  ExprMap table_expr_map;
  auto exprs = GenerateTableColumnValueExprs(alias, db_oid, tbl_oid);
  for (size_t idx = 0; idx < exprs.size(); idx++) {
    table_expr_map[exprs[idx]] = static_cast<int>(idx);
  }

  // Makes a copy
  auto *pred = parser::ExpressionUtil::EvaluateExpression({table_expr_map}, predicate_expr);
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
  auto predicate = GeneratePredicateForScan(conj_expr, op->GetTableAlias(), op->GetDatabaseOID(), op->GetTableOID());
  RegisterPointerCleanup<const parser::AbstractExpression>(predicate, true, true);
  delete conj_expr;

  // Check if we should do a parallel scan
  bool parallel = settings_->GetBool(settings::Param::parallel_execution);

  // OutputSchema
  auto output_schema = GenerateScanOutputSchema(op->GetTableOID());

  // Build
  output_plan_ = planner::SeqScanPlanNode::Builder()
                     .SetOutputSchema(std::move(output_schema))
                     .SetDatabaseOid(op->GetDatabaseOID())
                     .SetNamespaceOid(op->GetNamespaceOID())
                     .SetTableOid(op->GetTableOID())
                     .SetScanPredicate(predicate)
                     .SetColumnIds(std::move(column_ids))
                     .SetIsForUpdateFlag(op->GetIsForUpdate())
                     .SetIsParallelFlag(parallel)
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
  auto predicate = GeneratePredicateForScan(conj_expr, op->GetTableAlias(), op->GetDatabaseOID(), tbl_oid);
  RegisterPointerCleanup<const parser::AbstractExpression>(predicate, true, true);
  delete conj_expr;

  // Create index scan desc
  // Can destructively move from physical operator since we won't need them anymore
  auto tuple_oids = std::vector<catalog::col_oid_t>(op->GetKeyColumnOIDList());
  auto expr_list = std::vector<parser::ExpressionType>(op->GetExprTypeList());

  // We do a copy since const_cast is not good
  auto value_list = std::vector<type::TransientValue>(op->GetValueList().size());
  for (auto &value : op->GetValueList()) {
    auto val_copy = type::TransientValue(value);
    value_list.push_back(std::move(val_copy));
  }

  auto index_desc = planner::IndexScanDesc(std::move(tuple_oids), std::move(expr_list), std::move(value_list));

  // TODO(wz2): How do we derive the IsForUpdateFlag?
  output_plan_ = planner::IndexScanPlanNode::Builder()
                     .SetOutputSchema(std::move(output_schema))
                     .SetScanPredicate(predicate)
                     .SetIsForUpdateFlag(false)
                     .SetIsParallelFlag(false)
                     .SetDatabaseOid(op->GetDatabaseOID())
                     .SetNamespaceOid(op->GetNamespaceOID())
                     .SetIndexOid(op->GetIndexOID())
                     .SetColumnOids(std::move(column_ids))
                     .SetIndexScanDesc(std::move(index_desc))
                     .Build();
}

void PlanGenerator::Visit(const ExternalFileScan *op) {
  switch (op->GetFormat()) {
    case parser::ExternalFileFormat::CSV: {
      // First construct the output column descriptions
      std::vector<type::TypeId> value_types;
      std::vector<planner::OutputSchema::DerivedTarget> tl;
      std::vector<planner::OutputSchema::DirectMap> dml;
      std::vector<planner::OutputSchema::Column> out_cols;

      unsigned idx = 0;
      for (const auto *output_col : output_cols_) {
        dml.emplace_back(idx, std::make_pair(0u, idx));
        out_cols.emplace_back(output_col->GetExpressionName(), output_col->GetReturnValueType());
        value_types.push_back(output_col->GetReturnValueType());
        idx++;
      }

      auto output_schema = std::make_shared<planner::OutputSchema>(std::move(out_cols), std::move(tl), std::move(dml));
      output_plan_ = planner::CSVScanPlanNode::Builder()
                         .SetOutputSchema(std::move(output_schema))
                         .SetFileName(op->GetFilename())
                         .SetDelimiter(op->GetDelimiter())
                         .SetQuote(op->GetQuote())
                         .SetEscape(op->GetEscape())
                         .SetNullString(op->GetNullString())
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

  unsigned idx = 0;
  std::vector<planner::OutputSchema::DerivedTarget> tl;
  std::vector<planner::OutputSchema::DirectMap> dml;
  std::vector<planner::OutputSchema::Column> columns;
  for (auto &output : output_cols_) {
    // We can assert this based on InputColumnDeriver::Visit(const QueryDerivedScan *op)
    auto colve = dynamic_cast<const parser::ColumnValueExpression *>(output);
    TERRIER_ASSERT(colve != nullptr, "QueryDerivedScan output should be ColumnValueExpression");

    // Get offset into child_expr_ma
    auto expr = alias_expr_map.at(colve->GetColumnName()).get();
    auto offset = child_expr_map.at(expr);

    dml.emplace_back(idx, std::make_pair(0u, offset));
    columns.emplace_back(colve->GetColumnName(), colve->GetReturnValueType());
    idx++;
  }

  auto schema = std::make_shared<planner::OutputSchema>(std::move(columns), std::move(tl), std::move(dml));
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

  auto limit_out = output_plan_->GetOutputSchema()->Copy();
  auto &builder = planner::LimitPlanNode::Builder()
                      .SetOutputSchema(std::move(limit_out))
                      .SetLimit(op->GetLimit())
                      .SetOffset(op->GetOffset());

  if (!op->GetSortExpressions().empty()) {
    // Build order by clause
    TERRIER_ASSERT(children_expr_map_.size() == 1, "Limit needs 1 child expr map");
    auto &child_cols_map = children_expr_map_[0];

    TERRIER_ASSERT(op->GetSortExpressions().size() == op->GetSortAscending().size(),
                   "Limit sort expressions and sort ascending size should match");

    auto order_out = output_plan_->GetOutputSchema()->Copy();
    auto &order_build = planner::OrderByPlanNode::Builder()
                            .SetOutputSchema(std::move(order_out))
                            .AddChild(std::move(output_plan_))
                            .SetLimit(op->GetLimit())
                            .SetOffset(op->GetOffset());
    auto &sort_columns = op->GetSortExpressions();
    auto &sort_flags = op->GetSortAscending();
    auto sort_column_size = sort_columns.size();
    for (size_t idx = 0; idx < sort_column_size; idx++) {
      // Based on InputColumnDeriver, sort columns should be provided

      // Evaluate the sort_column using children_expr_map (what the child provides)
      // Need to replace ColumnValueExpression with DerivedValueExpression
      auto eval_expr = parser::ExpressionUtil::EvaluateExpression({child_cols_map}, sort_columns[idx].get());
      RegisterPointerCleanup<const parser::AbstractExpression>(eval_expr, true, true);
      order_build.AddSortKey(eval_expr, sort_flags[idx]);
    }

    output_plan_ = order_build.Build();
  }

  output_plan_ = builder.AddChild(std::move(output_plan_)).Build();
}

void PlanGenerator::Visit(UNUSED_ATTRIBUTE const OrderBy *op) {
  // OrderBy reorders tuples - should keep the same output schema as the original child plan
  TERRIER_ASSERT(children_plans_.size() == 1, "OrderBy needs 1 child plan");
  TERRIER_ASSERT(children_expr_map_.size() == 1, "OrderBy needs 1 child expr map");
  auto &child_cols_map = children_expr_map_[0];

  auto sort_prop = required_props_->GetPropertyOfType(PropertyType::SORT)->As<PropertySort>();
  TERRIER_ASSERT(sort_prop != nullptr, "OrderBy requires a sort property");

  auto sort_columns_size = sort_prop->GetSortColumnSize();
  auto &builder = planner::OrderByPlanNode::Builder().SetOutputSchema(children_plans_[0]->GetOutputSchema()->Copy());

  for (size_t i = 0; i < sort_columns_size; ++i) {
    auto sort_dir = sort_prop->GetSortAscending(static_cast<int>(i));
    auto key = sort_prop->GetSortColumn(i).get();

    // Evaluate the sort_column using children_expr_map (what the child provides)
    // Need to replace ColumnValueExpression with DerivedValueExpression
    auto eval_expr = parser::ExpressionUtil::EvaluateExpression({child_cols_map}, key);
    RegisterPointerCleanup<const parser::AbstractExpression>(eval_expr, true, true);
    builder.AddSortKey(eval_expr, sort_dir);
  }

  output_plan_ = builder.AddChild(std::move(children_plans_[0])).Build();
}

///////////////////////////////////////////////////////////////////////////////
// Distinct
///////////////////////////////////////////////////////////////////////////////

void PlanGenerator::Visit(UNUSED_ATTRIBUTE const Distinct *op) {
  // Now distinct is a flag in the parser, so we only support
  // distinct on all output columns
  TERRIER_ASSERT(children_expr_map_.size() == 1, "Distinct needs 1 child expr map");
  TERRIER_ASSERT(children_plans_.size() == 1, "Distinct needs 1 child plan");
  auto &child_expr_map = children_expr_map_[0];

  // Distinct output schema is defined by output cols...
  // Output_cols_ must be provided by the child plan (i.e child_expr_map_)
  std::vector<planner::OutputSchema::DerivedTarget> tl;
  std::vector<planner::OutputSchema::DirectMap> dml;
  std::vector<planner::OutputSchema::Column> output_schema_columns;
  std::vector<const parser::AbstractExpression *> hash_keys;
  unsigned idx = 0;
  for (auto &col : output_cols_) {
    auto col_offset = child_expr_map.at(col);

    // Create new DerivedValueExpression
    auto dve = new parser::DerivedValueExpression(col->GetReturnValueType(), 0, col_offset);
    RegisterPointerCleanup<const parser::DerivedValueExpression>(dve, true, true);
    hash_keys.push_back(dve);

    dml.emplace_back(idx, std::make_pair(0, col_offset));
    output_schema_columns.emplace_back(col->GetExpressionName(), col->GetReturnValueType());
    idx++;
  }

  auto schema =
      std::make_shared<planner::OutputSchema>(std::move(output_schema_columns), std::move(tl), std::move(dml));
  auto &builder =
      planner::HashPlanNode::Builder().SetOutputSchema(std::move(schema)).AddChild(std::move(children_plans_[0]));
  for (auto key : hash_keys) {
    builder.AddHashKey(key);
  }

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

std::shared_ptr<planner::OutputSchema> PlanGenerator::GenerateProjectionForJoin() {
  TERRIER_ASSERT(children_expr_map_.size() == 2, "Join needs 2 children");
  TERRIER_ASSERT(children_plans_.size() == 2, "Join needs 2 children");
  auto &l_child_expr_map = children_expr_map_[0];
  auto &r_child_expr_map = children_expr_map_[1];

  std::vector<planner::OutputSchema::DerivedTarget> tl;
  std::vector<planner::OutputSchema::DirectMap> dml;
  std::vector<planner::OutputSchema::Column> columns;
  size_t output_offset = 0;
  for (auto &expr : output_cols_) {
    auto col = planner::OutputSchema::Column(expr->GetExpressionName(), expr->GetReturnValueType());
    if (l_child_expr_map.count(expr) != 0u) {
      dml.emplace_back(output_offset, std::make_pair(0, l_child_expr_map[expr]));
    } else if (r_child_expr_map.count(expr) != 0u) {
      dml.emplace_back(output_offset, std::make_pair(1, r_child_expr_map[expr]));
    } else {
      // Pass owneship to DerivedColumn
      auto *eval = parser::ExpressionUtil::EvaluateExpression(children_expr_map_, expr);
      tl.emplace_back(output_offset, new planner::OutputSchema::DerivedColumn(col, eval));
    }

    columns.push_back(std::move(col));
    output_offset++;
  }

  return std::make_shared<planner::OutputSchema>(std::move(columns), std::move(tl), std::move(dml));
}

///////////////////////////////////////////////////////////////////////////////
// A NLJoin B (the join to not do on large relations)
///////////////////////////////////////////////////////////////////////////////

void PlanGenerator::Visit(const InnerNLJoin *op) {
  auto proj_schema = GenerateProjectionForJoin();

  auto comb_pred = parser::ExpressionUtil::JoinAnnotatedExprs(op->GetJoinPredicates());
  auto eval_pred = parser::ExpressionUtil::EvaluateExpression(children_expr_map_, comb_pred);
  auto join_predicate = parser::ExpressionUtil::ConvertExprCVNodes(eval_pred, children_expr_map_);
  delete comb_pred;
  delete eval_pred;
  RegisterPointerCleanup<const parser::AbstractExpression>(join_predicate, true, true);

  std::vector<const parser::AbstractExpression *> left_keys;
  std::vector<const parser::AbstractExpression *> right_keys;
  for (auto &expr : op->GetLeftKeys()) {
    auto left_key = parser::ExpressionUtil::EvaluateExpression({children_expr_map_[0]}, expr.get());
    RegisterPointerCleanup<const parser::AbstractExpression>(left_key, true, true);
    left_keys.push_back(left_key);
  }

  for (auto &expr : op->GetRightKeys()) {
    auto right_key = parser::ExpressionUtil::EvaluateExpression({children_expr_map_[1]}, expr.get());
    RegisterPointerCleanup<const parser::AbstractExpression>(right_key, true, true);
    right_keys.push_back(right_key);
  }

  output_plan_ = planner::NestedLoopJoinPlanNode::Builder()
                     .SetOutputSchema(std::move(proj_schema))
                     .SetJoinPredicate(join_predicate)
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
  auto eval_pred = parser::ExpressionUtil::EvaluateExpression(children_expr_map_, comb_pred);
  auto join_predicate = parser::ExpressionUtil::ConvertExprCVNodes(eval_pred, children_expr_map_);
  delete comb_pred;
  delete eval_pred;
  RegisterPointerCleanup<const parser::AbstractExpression>(join_predicate, true, true);

  auto &builder = planner::HashJoinPlanNode::Builder().SetOutputSchema(std::move(proj_schema));

  std::vector<ExprMap> l_child_map{std::move(children_expr_map_[0])};
  std::vector<ExprMap> r_child_map{std::move(children_expr_map_[1])};
  for (auto &expr : op->GetLeftKeys()) {
    auto left_key = parser::ExpressionUtil::EvaluateExpression(l_child_map, expr.get());
    RegisterPointerCleanup<const parser::AbstractExpression>(left_key, true, true);
    builder.AddLeftHashKey(left_key);
  }

  for (auto &expr : op->GetRightKeys()) {
    auto right_key = parser::ExpressionUtil::EvaluateExpression(r_child_map, expr.get());
    RegisterPointerCleanup<const parser::AbstractExpression>(right_key, true, true);
    builder.AddRightHashKey(right_key);
  }

  bool bloom = settings_->GetBool(settings::Param::build_bloom_filter);
  output_plan_ = builder.AddChild(std::move(children_plans_[0]))
                     .AddChild(std::move(children_plans_[1]))
                     .SetBuildBloomFilterFlag(bloom)
                     .Build();
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
    const parser::AbstractExpression *having_predicate) {
  TERRIER_ASSERT(children_expr_map_.size() == 1, "Aggregate needs 1 child plan");
  auto &child_expr_map = children_expr_map_[0];

  auto builder = planner::AggregatePlanNode::Builder();

  std::vector<planner::OutputSchema::Column> columns;
  std::vector<planner::OutputSchema::DerivedTarget> tl;
  std::vector<planner::OutputSchema::DirectMap> dml;

  auto agg_id = 0;
  ExprMap output_expr_map;
  for (size_t idx = 0; idx < output_cols_.size(); ++idx) {
    auto expr = output_cols_[idx];
    output_expr_map[expr] = static_cast<unsigned>(idx);

    const_cast<parser::AbstractExpression *>(expr)->DeriveReturnValueType();
    auto column_info = planner::OutputSchema::Column(expr->GetExpressionName(), expr->GetReturnValueType());
    if (parser::ExpressionUtil::IsAggregateExpression(expr->GetExpressionType())) {
      // We need to evaluate the expression first, convert ColumnValue => DerivedValue
      auto eval = parser::ExpressionUtil::EvaluateExpression(children_expr_map_, expr);

      // AggregatePlanNode owns AggregateTerm
      // RegisterPointerCleanup<const parser::AbstractExpression>(eval, true, true);
      TERRIER_ASSERT(parser::ExpressionUtil::IsAggregateExpression(eval->GetExpressionType()),
                     "Evaluated AggregateExpression should still be an aggregate expression");

      auto agg_expr = reinterpret_cast<const parser::AggregateExpression *>(eval);

      // Maps the aggregate value in the right tuple to the output
      // See aggregateor.cpp for more detail...
      // TODO([Execution Engine]): make sure this behavior still is correct
      dml.emplace_back(idx, std::make_pair(1, agg_id++));
      builder.AddAggregateTerm(const_cast<parser::AggregateExpression *>(agg_expr));
    } else if (child_expr_map.find(expr) != child_expr_map.end()) {
      dml.emplace_back(idx, std::make_pair(0, child_expr_map[expr]));
    } else {
      auto eval = parser::ExpressionUtil::EvaluateExpression(children_expr_map_, expr);
      tl.emplace_back(idx, new planner::OutputSchema::DerivedColumn(column_info, eval));
    }

    columns.push_back(std::move(column_info));
  }

  // Generate group by ids
  std::vector<unsigned> col_offsets;
  if (groupby_cols != nullptr) {
    for (auto &col : *groupby_cols) {
      col_offsets.push_back(child_expr_map.at(col.get()));
    }
  }

  // Generate the output schema
  auto output_schema = std::make_shared<planner::OutputSchema>(std::move(columns), std::move(tl), std::move(dml));

  // Prepare having clause
  auto eval_have = parser::ExpressionUtil::EvaluateExpression({output_expr_map}, having_predicate);
  auto predicate = parser::ExpressionUtil::ConvertExprCVNodes(eval_have, {output_expr_map});
  delete eval_have;

  // AggregatePlanNode owns HavingClausePredicate
  // RegisterPointerCleanup<const parser::AbstractExpression>(predicate, true, true);

  output_plan_ = builder.SetHavingClausePredicate(predicate)
                     .SetAggregateStrategyType(aggr_type)
                     .SetGroupByColOffsets(std::move(col_offsets))
                     .AddChild(std::move(children_plans_[0]))
                     .Build();
}

void PlanGenerator::Visit(const HashGroupBy *op) {
  auto having_predicates = parser::ExpressionUtil::JoinAnnotatedExprs(op->GetHaving());
  BuildAggregatePlan(planner::AggregateStrategyType::HASH, &op->GetColumns(), having_predicates);
  delete having_predicates;
}

void PlanGenerator::Visit(const SortGroupBy *op) {
  // Don't think this is ever visited since rule_impls.cpp does not create SortGroupBy
  auto having_predicates = parser::ExpressionUtil::JoinAnnotatedExprs(op->GetHaving());
  BuildAggregatePlan(planner::AggregateStrategyType::SORTED, &op->GetColumns(), having_predicates);
  delete having_predicates;
}

void PlanGenerator::Visit(UNUSED_ATTRIBUTE const Aggregate *op) {
  BuildAggregatePlan(planner::AggregateStrategyType::PLAIN, nullptr, nullptr);
}

///////////////////////////////////////////////////////////////////////////////
// Insert/Update/Delete
// To update or delete or select tuples, one must insert them first
///////////////////////////////////////////////////////////////////////////////

void PlanGenerator::Visit(const Insert *op) {
  auto &builder = planner::InsertPlanNode::Builder()
                      .SetDatabaseOid(op->GetDatabaseOid())
                      .SetNamespaceOid(op->GetNamespaceOid())
                      .SetTableOid(op->GetTableOid());

  auto values = op->GetValues();
  for (auto &tuple_value : values) {
    std::vector<const parser::AbstractExpression *> row(tuple_value.size());
    for (auto &col_value : tuple_value) row.push_back(col_value.get());

    builder.AddValues(std::move(row));
  }

  // The OutputSchema should match the schema of the underlying table...
  // Maybe for INSERT INTO tlb (col...) VALUES (...), we should match the specific column layout???
  // For now, output schema is the schema of the COMPLETE tuple of the underlying table
  auto &tbl_schema = accessor_->GetSchema(op->GetTableOid());
  auto &tbl_cols = tbl_schema.GetColumns();
  auto num_tbl_cols = tbl_cols.size();

  std::vector<planner::OutputSchema::DerivedTarget> tl;
  std::vector<planner::OutputSchema::DirectMap> dml;
  std::vector<planner::OutputSchema::Column> output_columns;
  for (auto idx = 0; idx < static_cast<int>(num_tbl_cols); idx++) {
    dml.emplace_back(idx, std::make_pair(0, idx));
    output_columns.emplace_back(tbl_cols[idx].Name(), tbl_cols[idx].Type());
  }
  auto output = std::make_shared<planner::OutputSchema>(std::move(output_columns), std::move(tl), std::move(dml));
  builder.SetOutputSchema(output);

  // This is based on what Peloton does/did with query_to_operator_transformer.cpp
  if (op->GetColumns().empty()) {
    // INSERT INTO tbl VALUES (...)
    // Generate column ids from underlying table
    for (auto idx = 0; idx < static_cast<int>(num_tbl_cols); idx++) {
      builder.AddParameterInfo(idx, tbl_cols[idx].Oid());
    }
  } else {
    // INSERT INTO tbl (col,....) VALUES (....)
    unsigned idx = 0;
    for (auto &col : op->GetColumns()) {
      builder.AddParameterInfo(idx, col);
      idx++;
    }
  }

  output_plan_ = builder.Build();
}

void PlanGenerator::Visit(const InsertSelect *op) {
  // Schema of InsertSelect is whatever the schema of the child is
  TERRIER_ASSERT(children_plans_.size() == 1, "InsertSelect needs 1 child plan");
  auto output_schema = children_plans_[0]->GetOutputSchema()->Copy();

  output_plan_ = planner::InsertPlanNode::Builder()
                     .SetOutputSchema(std::move(output_schema))
                     .SetDatabaseOid(op->GetDatabaseOid())
                     .SetNamespaceOid(op->GetNamespaceOid())
                     .SetTableOid(op->GetTableOid())
                     .AddChild(std::move(children_plans_[0]))
                     .Build();
}

void PlanGenerator::Visit(const Delete *op) {
  // OutputSchema of Delete is whatever the schema of the child is
  TERRIER_ASSERT(children_plans_.size() == 1, "Delete should have 1 child plan");
  auto output_schema = children_plans_[0]->GetOutputSchema()->Copy();

  output_plan_ = planner::DeletePlanNode::Builder()
                     .SetOutputSchema(std::move(output_schema))
                     .SetDatabaseOid(op->GetDatabaseOid())
                     .SetNamespaceOid(op->GetNamespaceOid())
                     .SetTableOid(op->GetTableOid())
                     .AddChild(std::move(children_plans_[0]))
                     .Build();
}

void PlanGenerator::Visit(const Update *op) {
  std::vector<planner::OutputSchema::DerivedTarget> tl;
  std::vector<planner::OutputSchema::DirectMap> dml;
  std::unordered_set<catalog::col_oid_t> update_col_offsets;

  const auto &tbl_alias = op->GetTableAlias();
  auto tbl_oid = op->GetTableOid();
  auto tbl_schema = accessor_->GetSchema(tbl_oid);

  // Construct column name => offset
  auto tbl_col_offset_map = std::unordered_map<std::string, unsigned>();
  auto idx = 0;
  for (auto &col : tbl_schema.GetColumns()) {
    tbl_col_offset_map[col.Name()] = idx;
    idx++;
  }

  ExprMap table_expr_map;
  auto exprs = GenerateTableColumnValueExprs(tbl_alias, op->GetDatabaseOid(), tbl_oid);
  for (idx = 0; idx < static_cast<int>(exprs.size()); ++idx) {
    table_expr_map[exprs[idx]] = idx;
  }

  // Initialize the vector so we can index into it
  std::vector<planner::OutputSchema::Column> output_columns(exprs.size());

  // Evaluate update expression and add to target list
  auto updates = op->GetUpdateClauses();
  for (auto &update : updates) {
    auto col_name = update->GetColumnName();
    auto col = tbl_schema.GetColumn(col_name);
    auto col_id = col.Oid();
    if (update_col_offsets.find(col_id) != update_col_offsets.end())
      throw SYNTAX_EXCEPTION("Multiple assignments to same column");

    update_col_offsets.insert(col_id);
    auto value = parser::ExpressionUtil::EvaluateExpression({table_expr_map}, update->GetUpdateValue().get());

    auto plan_col = planner::OutputSchema::Column(col_name, col.Type());

    // col_id in peloton just incremented from 0...
    // What we are looking for is the offset in the vector of schema columns
    tl.emplace_back(tbl_col_offset_map.at(col.Name()), new planner::OutputSchema::DerivedColumn(plan_col, value));
    output_columns[tbl_col_offset_map.at(col.Name())] = std::move(plan_col);
  }

  // Cleanup exprs and table_expr_map
  for (auto expr : exprs) {
    delete expr;
  }

  for (auto &column : tbl_schema.GetColumns()) {
    auto col_id = column.Oid();
    if (update_col_offsets.find(col_id) == update_col_offsets.end()) {
      unsigned offset = tbl_col_offset_map.at(column.Name());
      dml.emplace_back(offset, std::make_pair(0, offset));
      output_columns[offset] = planner::OutputSchema::Column(column.Name(), column.Type());
    }
  }

  // OutputSchema is basically a COMPLETE tuple of the underlying table.
  // By Peloton, direct map list are columns where there are basically no
  //   SET [..] = [..] clauses (point directly to underlying)
  // DerivedTarget are all the columns where there are SET [..] = [..] clauses
  auto output_schema =
      std::make_shared<planner::OutputSchema>(std::move(output_columns), std::move(tl), std::move(dml));

  // TODO(wz2): What is this SetUpdatePrimaryKey
  output_plan_ = planner::UpdatePlanNode::Builder()
                     .SetOutputSchema(output_schema)
                     .SetDatabaseOid(op->GetDatabaseOid())
                     .SetNamespaceOid(op->GetNamespaceOid())
                     .SetTableOid(op->GetTableOid())
                     .SetUpdatePrimaryKey(false)
                     .AddChild(std::move(children_plans_[0]))
                     .Build();
}

}  // namespace terrier::optimizer
