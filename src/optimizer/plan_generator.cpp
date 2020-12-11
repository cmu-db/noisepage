#include "optimizer/plan_generator.h"

#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "common/error/exception.h"
#include "execution/sql/value.h"
#include "optimizer/abstract_optimizer_node.h"
#include "optimizer/operator_node.h"
#include "optimizer/physical_operators.h"
#include "optimizer/properties.h"
#include "optimizer/property_set.h"
#include "optimizer/util.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression_util.h"
#include "planner/plannodes/aggregate_plan_node.h"
#include "planner/plannodes/analyze_plan_node.h"
#include "planner/plannodes/create_database_plan_node.h"
#include "planner/plannodes/create_function_plan_node.h"
#include "planner/plannodes/create_index_plan_node.h"
#include "planner/plannodes/create_namespace_plan_node.h"
#include "planner/plannodes/create_table_plan_node.h"
#include "planner/plannodes/create_trigger_plan_node.h"
#include "planner/plannodes/create_view_plan_node.h"
#include "planner/plannodes/csv_scan_plan_node.h"
#include "planner/plannodes/delete_plan_node.h"
#include "planner/plannodes/drop_database_plan_node.h"
#include "planner/plannodes/drop_index_plan_node.h"
#include "planner/plannodes/drop_namespace_plan_node.h"
#include "planner/plannodes/drop_table_plan_node.h"
#include "planner/plannodes/drop_trigger_plan_node.h"
#include "planner/plannodes/drop_view_plan_node.h"
#include "planner/plannodes/export_external_file_plan_node.h"
#include "planner/plannodes/hash_join_plan_node.h"
#include "planner/plannodes/index_join_plan_node.h"
#include "planner/plannodes/index_scan_plan_node.h"
#include "planner/plannodes/insert_plan_node.h"
#include "planner/plannodes/limit_plan_node.h"
#include "planner/plannodes/nested_loop_join_plan_node.h"
#include "planner/plannodes/order_by_plan_node.h"
#include "planner/plannodes/projection_plan_node.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "planner/plannodes/update_plan_node.h"
#include "settings/settings_manager.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"

namespace noisepage::optimizer {

PlanGenerator::PlanGenerator(common::ManagedPointer<planner::PlanMetaData> plan_meta_data)
    : plan_id_counter_(0), plan_meta_data_(plan_meta_data) {}

std::unique_ptr<planner::AbstractPlanNode> PlanGenerator::ConvertOpNode(
    transaction::TransactionContext *txn, catalog::CatalogAccessor *accessor, AbstractOptimizerNode *op,
    PropertySet *required_props, const std::vector<common::ManagedPointer<parser::AbstractExpression>> &required_cols,
    const std::vector<common::ManagedPointer<parser::AbstractExpression>> &output_cols,
    std::vector<std::unique_ptr<planner::AbstractPlanNode>> &&children_plans, std::vector<ExprMap> &&children_expr_map,
    const planner::PlanMetaData::PlanNodeMetaData &plan_node_meta_data) {
  required_props_ = required_props;
  required_cols_ = required_cols;
  output_cols_ = output_cols;
  children_plans_ = std::move(children_plans);
  children_expr_map_ = children_expr_map;
  accessor_ = accessor;
  plan_node_meta_data_ = plan_node_meta_data;
  txn_ = txn;

  op->Contents()->Accept(common::ManagedPointer<OperatorVisitor>(this));

  CorrectOutputPlanWithProjection();
  plan_meta_data_->AddPlanNodeMetaData(output_plan_->GetPlanNodeId(), plan_node_meta_data);
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

  auto schema = std::make_unique<planner::OutputSchema>(std::move(columns));
  if (output_plan_) {
    plan_meta_data_->AddPlanNodeMetaData(output_plan_->GetPlanNodeId(), plan_node_meta_data_);
  }
  auto builder = planner::ProjectionPlanNode::Builder();
  builder.SetOutputSchema(std::move(schema));
  builder.SetPlanNodeId(GetNextPlanNodeID());
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
std::vector<catalog::col_oid_t> PlanGenerator::GenerateColumnsForScan(const parser::AbstractExpression *predicate) {
  std::unordered_set<catalog::col_oid_t> unique_oids;
  for (auto &output_expr : output_cols_) {
    NOISEPAGE_ASSERT(output_expr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE,
                     "Scan columns should all be base table columns");

    auto tve = output_expr.CastManagedPointerTo<parser::ColumnValueExpression>();
    auto col_id = tve->GetColumnOid();

    NOISEPAGE_ASSERT(col_id != catalog::INVALID_COLUMN_OID, "TVE should be base");
    unique_oids.emplace(col_id);
  }
  // Add the oids contained in the scan.
  GenerateColumnsFromExpression(&unique_oids, predicate);

  // Make the output vector
  std::vector<catalog::col_oid_t> column_ids;
  column_ids.insert(column_ids.end(), unique_oids.begin(), unique_oids.end());
  return column_ids;
}

void PlanGenerator::GenerateColumnsFromExpression(std::unordered_set<catalog::col_oid_t> *oids,
                                                  const parser::AbstractExpression *expr) {
  if (expr == nullptr) return;
  if (expr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) {
    auto cve = static_cast<const parser::ColumnValueExpression *>(expr);
    oids->emplace(cve->GetColumnOid());
  }
  for (const auto &child_expr : expr->GetChildren()) {
    GenerateColumnsFromExpression(oids, child_expr.Get());
  }
}

std::unique_ptr<planner::OutputSchema> PlanGenerator::GenerateScanOutputSchema(catalog::table_oid_t tbl_oid) {
  // Underlying tuple provided by the table's schema.
  std::vector<planner::OutputSchema::Column> columns;
  for (auto &output_expr : output_cols_) {
    NOISEPAGE_ASSERT(output_expr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE,
                     "Scan columns should all be base table columns");

    auto tve = output_expr.CastManagedPointerTo<parser::ColumnValueExpression>();

    // output schema of a plan node is literally the columns of the table.
    // there is no such thing as an intermediate column here!
    columns.emplace_back(tve->GetColumnName(), tve->GetReturnValueType(), tve->Copy());
  }

  return std::make_unique<planner::OutputSchema>(std::move(columns));
}

void PlanGenerator::Visit(const SeqScan *op) {
  // Generate the predicate in the scan
  auto predicate = parser::ExpressionUtil::JoinAnnotatedExprs(op->GetPredicates()).release();
  RegisterPointerCleanup<parser::AbstractExpression>(predicate, true, true);

  // Generate output column IDs for plan
  std::vector<catalog::col_oid_t> column_ids = GenerateColumnsForScan(predicate);

  // OutputSchema
  auto output_schema = GenerateScanOutputSchema(op->GetTableOID());

  // Build
  output_plan_ = planner::SeqScanPlanNode::Builder()
                     .SetOutputSchema(std::move(output_schema))
                     .SetPlanNodeId(GetNextPlanNodeID())
                     .SetDatabaseOid(op->GetDatabaseOID())
                     .SetTableOid(op->GetTableOID())
                     .SetScanPredicate(common::ManagedPointer(predicate))
                     .SetColumnOids(std::move(column_ids))
                     .SetIsForUpdateFlag(op->GetIsForUpdate())
                     .Build();
}

void PlanGenerator::Visit(const IndexScan *op) {
  auto tbl_oid = op->GetTableOID();
  auto output_schema = GenerateScanOutputSchema(tbl_oid);
  uint64_t table_num_tuple = accessor_->GetTable(tbl_oid)->GetNumTuple();

  // Generate the predicate in the scan
  auto predicate = parser::ExpressionUtil::JoinAnnotatedExprs(op->GetPredicates()).release();
  RegisterPointerCleanup<parser::AbstractExpression>(predicate, true, true);

  // Generate ouptut column IDs for plan
  // An IndexScan (for now at least) will output all columns of its table
  std::vector<catalog::col_oid_t> column_ids = GenerateColumnsForScan(predicate);

  auto builder = planner::IndexScanPlanNode::Builder();
  builder.SetOutputSchema(std::move(output_schema));
  builder.SetPlanNodeId(GetNextPlanNodeID());
  builder.SetScanPredicate(common::ManagedPointer(predicate));
  builder.SetIsForUpdateFlag(op->GetIsForUpdate());
  builder.SetDatabaseOid(op->GetDatabaseOID());
  builder.SetIndexOid(op->GetIndexOID());
  builder.SetTableOid(tbl_oid);
  builder.SetColumnOids(std::move(column_ids));
  builder.SetTableNumTuple(table_num_tuple);
  builder.SetIndexSize(accessor_->GetTable(tbl_oid)->GetNumTuple());

  auto type = op->GetIndexScanType();
  builder.SetScanType(type);
  for (auto bound : op->GetBounds()) {
    if (type == planner::IndexScanType::Exact) {
      // Exact lookup
      builder.AddIndexColumn(bound.first, bound.second[0]);
    } else if (type == planner::IndexScanType::AscendingClosed) {
      // Range lookup, so use lo and hi
      builder.AddLoIndexColumn(bound.first, bound.second[0]);
      builder.AddHiIndexColumn(bound.first, bound.second[1]);
    } else if (type == planner::IndexScanType::AscendingOpenHigh) {
      // Open high scan, so use only lo
      builder.AddLoIndexColumn(bound.first, bound.second[0]);
    } else if (type == planner::IndexScanType::AscendingOpenLow) {
      // Open low scan, so use only high
      builder.AddHiIndexColumn(bound.first, bound.second[1]);
    } else if (type == planner::IndexScanType::AscendingOpenBoth) {
      // No bounds need to be set
    }
  }

  output_plan_ = builder.Build();
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
                         .SetPlanNodeId(GetNextPlanNodeID())
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
      NOISEPAGE_ASSERT(0, "Missing BinaryScanPlanNode");
    }
  }
}

void PlanGenerator::Visit(const QueryDerivedScan *op) {
  // QueryDerivedScan is a renaming layer...
  // This can be satisfied using a projection to correct the OutputSchema
  NOISEPAGE_ASSERT(children_plans_.size() == 1, "QueryDerivedScan must have 1 children plan");
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
                     .SetPlanNodeId(GetNextPlanNodeID())
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
  NOISEPAGE_ASSERT(children_plans_.size() == 1, "Limit needs 1 child plan");
  output_plan_ = std::move(children_plans_[0]);

  if (!op->GetSortExpressions().empty()) {
    // Build order by clause
    NOISEPAGE_ASSERT(children_expr_map_.size() == 1, "Limit needs 1 child expr map");
    auto &child_cols_map = children_expr_map_[0];

    NOISEPAGE_ASSERT(op->GetSortExpressions().size() == op->GetSortAscending().size(),
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
    order_build.SetPlanNodeId(GetNextPlanNodeID());
    order_build.SetOutputSchema(std::move(output_schema));
    order_build.AddChild(std::move(output_plan_));
    order_build.SetLimit(op->GetLimit());
    order_build.SetOffset(op->GetOffset());

    auto &sort_columns = op->GetSortExpressions();
    auto &sort_flags = op->GetSortAscending();
    auto sort_column_size = sort_columns.size();
    for (size_t i = 0; i < sort_column_size; i++) {
      // Based on InputColumnDeriver, sort columns should be provided

      // Evaluate the sort_column using children_expr_map (what the child provides)
      // Need to replace ColumnValueExpression with DerivedValueExpression
      auto eval_expr = parser::ExpressionUtil::EvaluateExpression({child_cols_map}, sort_columns[i]).release();
      RegisterPointerCleanup<parser::AbstractExpression>(eval_expr, true, true);
      order_build.AddSortKey(common::ManagedPointer(eval_expr), sort_flags[i]);
    }

    output_plan_ = order_build.Build();
    // Adding plan node meta data of other nodes is mainly handled in ConvertOpNode.
    // Need to call AddPlanNodeMetaData for Limit here because the limit node is not the output_plan_,
    // but an additional node generated in Visit.
    plan_meta_data_->AddPlanNodeMetaData(output_plan_->GetPlanNodeId(), plan_node_meta_data_);
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
                     .SetPlanNodeId(GetNextPlanNodeID())
                     .SetOutputSchema(std::move(limit_out))
                     .SetLimit(op->GetLimit())
                     .SetOffset(op->GetOffset())
                     .AddChild(std::move(output_plan_))
                     .Build();
}

void PlanGenerator::Visit(UNUSED_ATTRIBUTE const OrderBy *op) {
  // OrderBy reorders tuples - should keep the same output schema as the original child plan
  NOISEPAGE_ASSERT(children_plans_.size() == 1, "OrderBy needs 1 child plan");
  NOISEPAGE_ASSERT(children_expr_map_.size() == 1, "OrderBy needs 1 child expr map");
  auto &child_cols_map = children_expr_map_[0];

  auto sort_prop = required_props_->GetPropertyOfType(PropertyType::SORT)->As<PropertySort>();
  NOISEPAGE_ASSERT(sort_prop != nullptr, "OrderBy requires a sort property");

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
  builder.SetPlanNodeId(GetNextPlanNodeID());

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
  NOISEPAGE_ASSERT(children_plans_.size() == 1, "ExportExternalFile needs 1 child");

  output_plan_ = planner::ExportExternalFilePlanNode::Builder()
                     .AddChild(std::move(children_plans_[0]))
                     .SetPlanNodeId(GetNextPlanNodeID())
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
  NOISEPAGE_ASSERT(children_expr_map_.size() == 2, "Join needs 2 children");
  NOISEPAGE_ASSERT(children_plans_.size() == 2, "Join needs 2 children");
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
// A IndexJoin B (where B has an index)
///////////////////////////////////////////////////////////////////////////////

void PlanGenerator::Visit(const InnerIndexJoin *op) {
  NOISEPAGE_ASSERT(children_expr_map_.size() == 1, "InnerIndexJoin has 1 child");
  NOISEPAGE_ASSERT(children_plans_.size() == 1, "InnerIndexJoin has 1 child");

  std::vector<planner::OutputSchema::Column> columns;
  for (auto &expr : output_cols_) {
    auto type = expr->GetReturnValueType();
    if (children_expr_map_[0].count(expr) != 0U) {
      auto dve = std::make_unique<parser::DerivedValueExpression>(type, 0, children_expr_map_[0][expr]);
      columns.emplace_back(expr->GetExpressionName(), type, std::move(dve));
    } else {
      auto eval = parser::ExpressionUtil::EvaluateExpression(children_expr_map_, expr);
      columns.emplace_back(expr->GetExpressionName(), type, std::move(eval));
    }
  }

  auto proj_schema = std::make_unique<planner::OutputSchema>(std::move(columns));
  auto comb_pred = parser::ExpressionUtil::JoinAnnotatedExprs(op->GetJoinPredicates());
  auto eval_pred = parser::ExpressionUtil::EvaluateExpression(children_expr_map_, common::ManagedPointer(comb_pred));
  auto join_predicate =
      parser::ExpressionUtil::ConvertExprCVNodes(common::ManagedPointer(eval_pred), children_expr_map_).release();
  RegisterPointerCleanup<parser::AbstractExpression>(join_predicate, true, true);

  auto type = op->GetScanType();
  planner::IndexJoinPlanNode::Builder builder;
  builder.SetOutputSchema(std::move(proj_schema))
      .SetPlanNodeId(GetNextPlanNodeID())
      .SetJoinType(planner::LogicalJoinType::INNER)
      .SetJoinPredicate(common::ManagedPointer(join_predicate))
      .SetIndexOid(op->GetIndexOID())
      .SetTableOid(op->GetTableOID())
      .SetScanType(op->GetScanType())
      .SetIndexSize(accessor_->GetTable(op->GetTableOID())->GetNumTuple())
      .AddChild(std::move(children_plans_[0]));

  for (auto bound : op->GetJoinKeys()) {
    if (type == planner::IndexScanType::Exact) {
      // Exact lookup
      auto key = parser::ExpressionUtil::EvaluateExpression(children_expr_map_, bound.second[0]).release();
      RegisterPointerCleanup<parser::AbstractExpression>(key, true, true);

      builder.AddLoIndexColumn(bound.first, common::ManagedPointer(key));
      builder.AddHiIndexColumn(bound.first, common::ManagedPointer(key));
    } else if (type == planner::IndexScanType::AscendingClosed) {
      // Range lookup, so use lo and hi
      auto lkey = parser::ExpressionUtil::EvaluateExpression(children_expr_map_, bound.second[0]).release();
      auto hkey = parser::ExpressionUtil::EvaluateExpression(children_expr_map_, bound.second[1]).release();
      RegisterPointerCleanup<parser::AbstractExpression>(lkey, true, true);
      RegisterPointerCleanup<parser::AbstractExpression>(hkey, true, true);

      builder.AddLoIndexColumn(bound.first, common::ManagedPointer(lkey));
      builder.AddHiIndexColumn(bound.first, common::ManagedPointer(hkey));
    } else if (type == planner::IndexScanType::AscendingOpenHigh) {
      // Open high scan, so use only lo
      auto key = parser::ExpressionUtil::EvaluateExpression(children_expr_map_, bound.second[0]).release();
      RegisterPointerCleanup<parser::AbstractExpression>(key, true, true);
      builder.AddLoIndexColumn(bound.first, common::ManagedPointer(key));
    } else if (type == planner::IndexScanType::AscendingOpenLow) {
      // Open low scan, so use only high
      auto key = parser::ExpressionUtil::EvaluateExpression(children_expr_map_, bound.second[1]).release();
      RegisterPointerCleanup<parser::AbstractExpression>(key, true, true);
      builder.AddHiIndexColumn(bound.first, common::ManagedPointer(key));
    } else if (type == planner::IndexScanType::AscendingOpenBoth) {
      // No bounds need to be set
      NOISEPAGE_ASSERT(0, "Unreachable");
    }
  }

  output_plan_ = builder.Build();
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

  output_plan_ = planner::NestedLoopJoinPlanNode::Builder()
                     .SetOutputSchema(std::move(proj_schema))
                     .SetPlanNodeId(GetNextPlanNodeID())
                     .SetJoinPredicate(common::ManagedPointer(join_predicate))
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .AddChild(std::move(children_plans_[0]))
                     .AddChild(std::move(children_plans_[1]))
                     .Build();
}

void PlanGenerator::Visit(UNUSED_ATTRIBUTE const LeftNLJoin *op) { NOISEPAGE_ASSERT(0, "LeftNLJoin not implemented"); }

void PlanGenerator::Visit(UNUSED_ATTRIBUTE const RightNLJoin *op) {
  NOISEPAGE_ASSERT(0, "RightNLJoin not implemented");
}

void PlanGenerator::Visit(UNUSED_ATTRIBUTE const OuterNLJoin *op) {
  NOISEPAGE_ASSERT(0, "OuterNLJoin not implemented");
}

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
  builder.SetPlanNodeId(GetNextPlanNodeID());

  for (auto &expr : op->GetLeftKeys()) {
    auto left_key = parser::ExpressionUtil::EvaluateExpression(children_expr_map_, expr).release();
    RegisterPointerCleanup<parser::AbstractExpression>(left_key, true, true);
    builder.AddLeftHashKey(common::ManagedPointer(left_key));
  }

  for (auto &expr : op->GetRightKeys()) {
    auto right_key = parser::ExpressionUtil::EvaluateExpression(children_expr_map_, expr).release();
    RegisterPointerCleanup<parser::AbstractExpression>(right_key, true, true);
    builder.AddRightHashKey(common::ManagedPointer(right_key));
  }

  builder.AddChild(std::move(children_plans_[0]));
  builder.AddChild(std::move(children_plans_[1]));
  builder.SetJoinPredicate(common::ManagedPointer(join_predicate));
  builder.SetJoinType(planner::LogicalJoinType::INNER);
  output_plan_ = builder.Build();
}

void PlanGenerator::Visit(const LeftHashJoin *op) {
  auto proj_schema = GenerateProjectionForJoin();

  auto comb_pred = parser::ExpressionUtil::JoinAnnotatedExprs(op->GetJoinPredicates());
  auto eval_pred =
      parser::ExpressionUtil::EvaluateExpression(children_expr_map_, common::ManagedPointer(comb_pred.get()));
  auto join_predicate =
      parser::ExpressionUtil::ConvertExprCVNodes(common::ManagedPointer(eval_pred.get()), children_expr_map_).release();
  RegisterPointerCleanup<parser::AbstractExpression>(join_predicate, true, true);

  auto builder = planner::HashJoinPlanNode::Builder();
  builder.SetOutputSchema(std::move(proj_schema));
  builder.SetPlanNodeId(GetNextPlanNodeID());

  for (auto &expr : op->GetLeftKeys()) {
    auto left_key = parser::ExpressionUtil::EvaluateExpression(children_expr_map_, expr).release();
    RegisterPointerCleanup<parser::AbstractExpression>(left_key, true, true);
    builder.AddLeftHashKey(common::ManagedPointer(left_key));
  }

  for (auto &expr : op->GetRightKeys()) {
    auto right_key = parser::ExpressionUtil::EvaluateExpression(children_expr_map_, expr).release();
    RegisterPointerCleanup<parser::AbstractExpression>(right_key, true, true);
    builder.AddRightHashKey(common::ManagedPointer(right_key));
  }

  builder.AddChild(std::move(children_plans_[0]));
  builder.AddChild(std::move(children_plans_[1]));
  builder.SetJoinPredicate(common::ManagedPointer(join_predicate));
  builder.SetJoinType(planner::LogicalJoinType::LEFT);
  output_plan_ = builder.Build();
}

void PlanGenerator::Visit(UNUSED_ATTRIBUTE const RightHashJoin *op) {
  NOISEPAGE_ASSERT(0, "RightHashJoin not implemented");
}

void PlanGenerator::Visit(UNUSED_ATTRIBUTE const OuterHashJoin *op) {
  NOISEPAGE_ASSERT(0, "OuterHashJoin not implemented");
}

///////////////////////////////////////////////////////////////////////////////////////////////
// A left semi hashjoin B (what you should do for large relations, A should be smaller than B)
///////////////////////////////////////////////////////////////////////////////////////////////

void PlanGenerator::Visit(const LeftSemiHashJoin *op) {
  auto proj_schema = GenerateProjectionForJoin();

  auto comb_pred = parser::ExpressionUtil::JoinAnnotatedExprs(op->GetJoinPredicates());
  auto eval_pred =
      parser::ExpressionUtil::EvaluateExpression(children_expr_map_, common::ManagedPointer(comb_pred.get()));
  auto join_predicate =
      parser::ExpressionUtil::ConvertExprCVNodes(common::ManagedPointer(eval_pred.get()), children_expr_map_).release();
  RegisterPointerCleanup<parser::AbstractExpression>(join_predicate, true, true);

  auto builder = planner::HashJoinPlanNode::Builder();
  builder.SetOutputSchema(std::move(proj_schema));
  builder.SetPlanNodeId(GetNextPlanNodeID());

  for (auto &expr : op->GetLeftKeys()) {
    auto left_key = parser::ExpressionUtil::EvaluateExpression(children_expr_map_, expr).release();
    RegisterPointerCleanup<parser::AbstractExpression>(left_key, true, true);
    builder.AddLeftHashKey(common::ManagedPointer(left_key));
  }

  for (auto &expr : op->GetRightKeys()) {
    auto right_key = parser::ExpressionUtil::EvaluateExpression(children_expr_map_, expr).release();
    RegisterPointerCleanup<parser::AbstractExpression>(right_key, true, true);
    builder.AddRightHashKey(common::ManagedPointer(right_key));
  }

  builder.AddChild(std::move(children_plans_[0]));
  builder.AddChild(std::move(children_plans_[1]));
  builder.SetJoinPredicate(common::ManagedPointer(join_predicate));
  builder.SetJoinType(planner::LogicalJoinType::LEFT_SEMI);
  output_plan_ = builder.Build();
}

///////////////////////////////////////////////////////////////////////////////
// Aggregations (when the groups are greater than individuals)
///////////////////////////////////////////////////////////////////////////////

void PlanGenerator::BuildAggregatePlan(
    planner::AggregateStrategyType aggr_type,
    const std::vector<common::ManagedPointer<parser::AbstractExpression>> *groupby_cols,
    common::ManagedPointer<parser::AbstractExpression> having_predicate) {
  NOISEPAGE_ASSERT(children_expr_map_.size() == 1, "Aggregate needs 1 child plan");
  auto &child_expr_map = children_expr_map_[0];
  auto builder = planner::AggregatePlanNode::Builder();

  // Generate group by ids
  ExprMap gb_map;
  if (groupby_cols != nullptr) {
    uint32_t offset = 0;
    for (auto &col : *groupby_cols) {
      gb_map[col] = offset;

      auto eval = parser::ExpressionUtil::EvaluateExpression({child_expr_map}, col);
      auto gb_term =
          parser::ExpressionUtil::ConvertExprCVNodes(common::ManagedPointer(eval), {child_expr_map}).release();
      RegisterPointerCleanup<parser::AbstractExpression>(gb_term, true, true);
      builder.AddGroupByTerm(common::ManagedPointer(gb_term));
      offset++;
    }
  }

  auto agg_id = 0;
  /* Each ExprMap represents a single group of tuples (i.e. one relation). For aggregates we manually output all the
   * group by columns into the 0th relation and all the aggregate values into the 1st relation. Therefore we need a
   * separate map for each.
   */
  ExprMap gb_output_expr_map;
  ExprMap agg_output_expr_map;
  std::vector<planner::OutputSchema::Column> columns;
  for (size_t idx = 0; idx < output_cols_.size(); ++idx) {
    auto expr = output_cols_[idx];
    expr->DeriveReturnValueType();

    if (parser::ExpressionUtil::IsAggregateExpression(expr->GetExpressionType())) {
      agg_output_expr_map[expr] = static_cast<unsigned>(agg_id);
      // We need to evaluate the expression first, convert ColumnValue => DerivedValue
      auto eval = parser::ExpressionUtil::EvaluateExpression(children_expr_map_, expr).release();
      NOISEPAGE_ASSERT(parser::ExpressionUtil::IsAggregateExpression(eval->GetExpressionType()),
                       "Evaluated AggregateExpression should still be an aggregate expression");

      auto agg_expr = reinterpret_cast<parser::AggregateExpression *>(eval);
      RegisterPointerCleanup<parser::AggregateExpression>(agg_expr, true, true);
      builder.AddAggregateTerm(common::ManagedPointer(agg_expr));

      // Maps the aggregate value in the right tuple to the output
      auto dve = std::make_unique<parser::DerivedValueExpression>(expr->GetReturnValueType(), 1, agg_id++);
      columns.emplace_back(expr->GetExpressionName(), expr->GetReturnValueType(), std::move(dve));
    } else if (gb_map.find(expr) != gb_map.end()) {
      gb_output_expr_map[expr] = static_cast<unsigned>(idx - agg_id);
      auto dve = std::make_unique<parser::DerivedValueExpression>(expr->GetReturnValueType(), 0, gb_map[expr]);
      columns.emplace_back(expr->GetExpressionName(), expr->GetReturnValueType(), std::move(dve));
    }
  }

  // Generate the output schema
  auto output_schema = std::make_unique<planner::OutputSchema>(std::move(columns));

  // Prepare having clause
  auto eval_have =
      parser::ExpressionUtil::EvaluateExpression({gb_output_expr_map, agg_output_expr_map}, having_predicate);
  auto predicate = parser::ExpressionUtil::ConvertExprCVNodes(common::ManagedPointer(eval_have),
                                                              {gb_output_expr_map, agg_output_expr_map})
                       .release();
  RegisterPointerCleanup<parser::AbstractExpression>(predicate, true, true);

  builder.SetOutputSchema(std::move(output_schema));
  builder.SetPlanNodeId(GetNextPlanNodeID());
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
  builder.SetPlanNodeId(GetNextPlanNodeID());
  builder.SetDatabaseOid(op->GetDatabaseOid());
  builder.SetTableOid(op->GetTableOid());

  auto tbl_oid = op->GetTableOid();
  std::vector<catalog::index_oid_t> indexes = accessor_->GetIndexOids(tbl_oid);
  builder.SetIndexOids(std::move(indexes));

  auto values = op->GetValues();
  for (auto &tuple_value : values) {
    builder.AddValues(std::move(tuple_value));
  }

  // This is based on what Peloton does/did with query_to_operator_transformer.cpp
  NOISEPAGE_ASSERT(!op->GetColumns().empty(), "Transformer should added columns");
  for (auto &col : op->GetColumns()) {
    builder.AddParameterInfo(col);
  }

  // Schema of Insert is empty
  builder.SetOutputSchema(std::make_unique<planner::OutputSchema>());
  output_plan_ = builder.Build();
}

void PlanGenerator::Visit(const InsertSelect *op) {
  // Schema of Insert is empty
  NOISEPAGE_ASSERT(children_plans_.size() == 1, "InsertSelect needs 1 child plan");
  auto output_schema = std::make_unique<planner::OutputSchema>();

  auto tbl_oid = op->GetTableOid();
  std::vector<catalog::index_oid_t> indexes = accessor_->GetIndexOids(tbl_oid);
  output_plan_ = planner::InsertPlanNode::Builder()
                     .SetOutputSchema(std::move(output_schema))
                     .SetPlanNodeId(GetNextPlanNodeID())
                     .SetDatabaseOid(op->GetDatabaseOid())
                     .SetTableOid(op->GetTableOid())
                     .SetIndexOids(std::move(indexes))
                     .AddChild(std::move(children_plans_[0]))
                     .Build();
}

void PlanGenerator::Visit(const Delete *op) {
  // OutputSchema of DELETE is empty vector
  NOISEPAGE_ASSERT(children_plans_.size() == 1, "Delete should have 1 child plan");
  auto output_schema = std::make_unique<planner::OutputSchema>();

  auto tbl_oid = op->GetTableOid();
  std::vector<catalog::index_oid_t> indexes = accessor_->GetIndexOids(tbl_oid);
  output_plan_ = planner::DeletePlanNode::Builder()
                     .SetPlanNodeId(GetNextPlanNodeID())
                     .SetOutputSchema(std::move(output_schema))
                     .SetDatabaseOid(op->GetDatabaseOid())
                     .SetTableOid(op->GetTableOid())
                     .SetIndexOids(std::move(indexes))
                     .AddChild(std::move(children_plans_[0]))
                     .Build();
}

void PlanGenerator::Visit(const Update *op) {
  auto builder = planner::UpdatePlanNode::Builder();
  std::unordered_set<catalog::col_oid_t> update_col_offsets;

  auto tbl_oid = op->GetTableOid();
  auto tbl_schema = accessor_->GetSchema(tbl_oid);

  auto indexes = accessor_->GetIndexes(op->GetTableOid());
  ExprSet cves;
  for (auto index : indexes) {
    for (auto &column : index.second.GetColumns()) {
      // TODO(tanujnay112) big cheating with the const_cast but as the todo in abstract_expression.h says
      // these are supposed to be immutable anyway. We need to either go around consting everything or document
      // this assumption better somewhere
      parser::ExpressionUtil::GetTupleValueExprs(
          &cves, common::ManagedPointer(const_cast<parser::AbstractExpression *>(column.StoredExpression().Get())));
    }
  }

  std::unordered_set<std::string> update_column_names;

  // Evaluate update expression and add to target list
  auto updates = op->GetUpdateClauses();
  for (auto &update : updates) {
    auto col = tbl_schema.GetColumn(update->GetColumnName());
    auto col_id = col.Oid();
    if (update_col_offsets.find(col_id) != update_col_offsets.end())
      throw SYNTAX_EXCEPTION("Multiple assignments to same column");

    // We need to EvaluateExpression since column value expressions in the update
    // clause should refer to column values coming from the child.
    update_col_offsets.insert(col_id);
    auto upd_value = parser::ExpressionUtil::EvaluateExpression(children_expr_map_, update->GetUpdateValue()).release();
    builder.AddSetClause(std::make_pair(col_id, common::ManagedPointer(upd_value)));

    update_column_names.insert(update->GetColumnName());
    RegisterPointerCleanup<parser::AbstractExpression>(upd_value, true, true);
  }

  bool indexed_update = false;

  // TODO(tanujnay112) can optimize if we stored updated column oids in the update nodes during binding
  // such that we didn't have to store string sets
  for (auto &cve : cves) {
    if (update_column_names.find(cve.CastManagedPointerTo<parser::ColumnValueExpression>()->GetColumnName()) !=
        update_column_names.end()) {
      indexed_update = true;
      break;
    }
  }

  std::vector<catalog::index_oid_t> index_oids;
  if (indexed_update) {
    // Since an indexed_update will delete the tuple, we will have to codegen
    // IndexDelete and IndexInsert for all indexes.
    index_oids = accessor_->GetIndexOids(op->GetTableOid());
  }

  // Empty OutputSchema for update
  auto output_schema = std::make_unique<planner::OutputSchema>();

  // TODO(wz2): What is this SetUpdatePrimaryKey
  output_plan_ = builder.SetOutputSchema(std::move(output_schema))
                     .SetPlanNodeId(GetNextPlanNodeID())
                     .SetDatabaseOid(op->GetDatabaseOid())
                     .SetTableOid(op->GetTableOid())
                     .SetIndexedUpdate(indexed_update)
                     .SetUpdatePrimaryKey(false)
                     .SetIndexOids(std::move(index_oids))
                     .AddChild(std::move(children_plans_[0]))
                     .Build();
}

// Create/Drop

void PlanGenerator::Visit(const CreateDatabase *create_database) {
  output_plan_ = planner::CreateDatabasePlanNode::Builder().SetDatabaseName(create_database->GetDatabaseName()).Build();
}

void PlanGenerator::Visit(const CreateFunction *create_function) {
  output_plan_ = planner::CreateFunctionPlanNode::Builder()
                     .SetPlanNodeId(GetNextPlanNodeID())
                     .SetDatabaseOid(create_function->GetDatabaseOid())
                     .SetNamespaceOid(create_function->GetNamespaceOid())
                     .SetFunctionName(create_function->GetFunctionName())
                     .SetLanguage(create_function->GetUDFLanguage())
                     .SetBody(create_function->GetFunctionBody())
                     .SetFunctionParamNames(create_function->GetFunctionParameterNames())
                     .SetFunctionParamTypes(create_function->GetFunctionParameterTypes())
                     .SetReturnType(create_function->GetReturnType())
                     .SetIsReplace(create_function->IsReplace())
                     .SetParamCount(create_function->GetParamCount())
                     .Build();
}

void PlanGenerator::Visit(const CreateIndex *create_index) {
  // Copy the IndexSchema out
  auto schema = create_index->GetSchema();
  std::vector<catalog::IndexSchema::Column> cols;
  for (const auto &col : schema->GetColumns()) {
    cols.emplace_back(col);
  }
  auto idx_schema = std::make_unique<catalog::IndexSchema>(std::move(cols), schema->Type(), schema->Unique(),
                                                           schema->Primary(), schema->Exclusion(), schema->Immediate());
  auto out_schema = std::make_unique<planner::OutputSchema>();

  output_plan_ = planner::CreateIndexPlanNode::Builder()
                     .SetPlanNodeId(GetNextPlanNodeID())
                     .SetNamespaceOid(create_index->GetNamespaceOid())
                     .SetTableOid(create_index->GetTableOid())
                     .SetIndexName(create_index->GetIndexName())
                     .SetSchema(std::move(idx_schema))
                     .SetOutputSchema(std::move(out_schema))
                     .Build();
}

void PlanGenerator::Visit(const CreateTable *create_table) {
  auto builder = planner::CreateTablePlanNode::Builder();
  builder.SetPlanNodeId(GetNextPlanNodeID());
  builder.SetNamespaceOid(create_table->GetNamespaceOid());
  builder.SetTableName(create_table->GetTableName());
  builder.SetBlockStore(accessor_->GetBlockStore());

  std::vector<std::string> pk_cols;
  std::string pk_cname = create_table->GetTableName() + "_pk";
  std::vector<catalog::Schema::Column> cols;
  for (auto col : create_table->GetColumns()) {
    if (col->IsPrimaryKey()) {
      pk_cols.push_back(col->GetColumnName());
      pk_cname += "_" + col->GetColumnName();
    }

    // Unique Constraint
    if (col->IsUnique()) {
      builder.ProcessUniqueConstraint(col);
    }

    // Check Constraint
    if (col->GetCheckExpression()) {
      builder.ProcessCheckConstraint(col);
    }

    auto val_type = col->GetValueType();

    parser::ConstantValueExpression null_val{val_type, execution::sql::Val(true)};
    auto &val = col->GetDefaultExpression() != nullptr ? *col->GetDefaultExpression() : null_val;

    if (val_type == type::TypeId::VARCHAR || val_type == type::TypeId::VARBINARY) {
      cols.emplace_back(col->GetColumnName(), val_type, col->GetTypeModifier(), col->IsNullable(), val);
    } else {
      cols.emplace_back(col->GetColumnName(), val_type, col->IsNullable(), val);
    }
  }

  // Process FK
  for (auto fk : create_table->GetForeignKeys()) {
    builder.ProcessForeignKeyConstraint(create_table->GetTableName(), fk);
  }

  // Set PK info
  if (!pk_cols.empty()) {
    builder.SetHasPrimaryKey(true);
    builder.SetPrimaryKey(
        planner::PrimaryKeyInfo{.primary_key_cols_ = std::move(pk_cols), .constraint_name_ = std::move(pk_cname)});
  }

  auto schema = std::make_unique<catalog::Schema>(std::move(cols));
  builder.SetTableSchema(std::move(schema));
  output_plan_ = builder.Build();
}

void PlanGenerator::Visit(const CreateNamespace *create_namespace) {
  output_plan_ =
      planner::CreateNamespacePlanNode::Builder().SetNamespaceName(create_namespace->GetNamespaceName()).Build();
}

void PlanGenerator::Visit(const CreateTrigger *create_trigger) {
  output_plan_ = planner::CreateTriggerPlanNode::Builder()
                     .SetPlanNodeId(GetNextPlanNodeID())
                     .SetDatabaseOid(create_trigger->GetDatabaseOid())
                     .SetNamespaceOid(create_trigger->GetNamespaceOid())
                     .SetTableOid(create_trigger->GetTableOid())
                     .SetTriggerName(create_trigger->GetTriggerName())
                     .SetTriggerFuncnames(create_trigger->GetTriggerFuncName())
                     .SetTriggerArgs(create_trigger->GetTriggerArgs())
                     .SetTriggerColumns(create_trigger->GetTriggerColumns())
                     .SetTriggerWhen(create_trigger->GetTriggerWhen())
                     .SetTriggerType(create_trigger->GetTriggerType())
                     .Build();
}

void PlanGenerator::Visit(const CreateView *create_view) {
  output_plan_ = planner::CreateViewPlanNode::Builder()
                     .SetPlanNodeId(GetNextPlanNodeID())
                     .SetDatabaseOid(create_view->GetDatabaseOid())
                     .SetNamespaceOid(create_view->GetNamespaceOid())
                     .SetViewName(create_view->GetViewName())
                     .SetViewQuery(create_view->GetViewQuery()->Copy())
                     .Build();
}

void PlanGenerator::Visit(const DropDatabase *drop_database) {
  output_plan_ = planner::DropDatabasePlanNode::Builder()
                     .SetDatabaseOid(drop_database->GetDatabaseOID())
                     .SetPlanNodeId(GetNextPlanNodeID())
                     .Build();
}

void PlanGenerator::Visit(const DropTable *drop_table) {
  output_plan_ = planner::DropTablePlanNode::Builder()
                     .SetTableOid(drop_table->GetTableOID())
                     .SetPlanNodeId(GetNextPlanNodeID())
                     .Build();
}

void PlanGenerator::Visit(const DropIndex *drop_index) {
  output_plan_ = planner::DropIndexPlanNode::Builder()
                     .SetIndexOid(drop_index->GetIndexOID())
                     .SetPlanNodeId(GetNextPlanNodeID())
                     .Build();
}

void PlanGenerator::Visit(const DropNamespace *drop_namespace) {
  output_plan_ = planner::DropNamespacePlanNode::Builder()
                     .SetNamespaceOid(drop_namespace->GetNamespaceOID())
                     .SetPlanNodeId(GetNextPlanNodeID())
                     .Build();
}

void PlanGenerator::Visit(const DropTrigger *drop_trigger) {
  output_plan_ = planner::DropTriggerPlanNode::Builder()
                     .SetPlanNodeId(GetNextPlanNodeID())
                     .SetDatabaseOid(drop_trigger->GetDatabaseOid())
                     .SetNamespaceOid(drop_trigger->GetNamespaceOid())
                     .SetTriggerOid(drop_trigger->GetTriggerOid())
                     .SetIfExist(drop_trigger->IsIfExists())
                     .Build();
}

void PlanGenerator::Visit(const DropView *drop_view) {
  output_plan_ = planner::DropViewPlanNode::Builder()
                     .SetPlanNodeId(GetNextPlanNodeID())
                     .SetDatabaseOid(drop_view->GetDatabaseOid())
                     .SetViewOid(drop_view->GetViewOid())
                     .SetIfExist(drop_view->IsIfExists())
                     .Build();
}

void PlanGenerator::Visit(const Analyze *analyze) {
  output_plan_ = planner::AnalyzePlanNode::Builder()
                     .SetPlanNodeId(GetNextPlanNodeID())
                     .SetDatabaseOid(analyze->GetDatabaseOid())
                     .SetTableOid(analyze->GetTableOid())
                     .SetColumnOIDs(analyze->GetColumns())
                     .Build();
}

}  // namespace noisepage::optimizer
