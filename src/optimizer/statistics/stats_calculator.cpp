#include "optimizer/statistics/stats_calculator.h"

#include <algorithm>
#include <cmath>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "optimizer/logical_operators.h"
#include "optimizer/memo.h"
#include "optimizer/optimizer_context.h"
#include "optimizer/physical_operators.h"
#include "optimizer/statistics/column_stats.h"
#include "optimizer/statistics/selectivity.h"
#include "optimizer/statistics/stats_storage.h"
#include "optimizer/statistics/table_stats.h"
#include "optimizer/statistics/value_condition.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression_util.h"

namespace noisepage::optimizer {

void StatsCalculator::CalculateStats(GroupExpression *gexpr, ExprSet required_cols, OptimizerContext *context) {
  gexpr_ = gexpr;
  required_cols_ = std::move(required_cols);
  context_ = context;
  gexpr->Contents()->Accept(common::ManagedPointer<OperatorVisitor>(this));
}

void StatsCalculator::Visit(const LogicalGet *op) {
  if (op->GetTableOid() == catalog::INVALID_TABLE_OID) {
    // Dummy scan
    return;
  }

  auto root_group = context_->GetMemo().GetGroupByID(gexpr_->GetGroupID());
  auto table_stats = context_->GetStatsStorage()->GetTableStats(op->GetDatabaseOid(), op->GetTableOid());
  if (table_stats == nullptr) {
    // no table stats
    // Fill with defaults to prevent an infinite loop from above
    for (auto &col : required_cols_) {
      NOISEPAGE_ASSERT(col->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE, "Expected ColumnValue");
      auto tv_expr = col.CastManagedPointerTo<parser::ColumnValueExpression>();
      root_group->AddStats(tv_expr->GetFullName(), CreateDefaultStats(tv_expr));
    }
    return;
  }

  // First, get the required stats of the base table
  std::unordered_map<std::string, std::unique_ptr<ColumnStats>> required_stats;
  for (auto &col : required_cols_) {
    // Make a copy for required stats since we may want to modify later
    AddBaseTableStats(col, table_stats, &required_stats);
  }

  // Compute selectivity at the first time
  if (root_group->GetNumRows() == -1) {
    std::unordered_map<std::string, std::unique_ptr<ColumnStats>> predicate_stats;
    for (auto &annotated_expr : op->GetPredicates()) {
      ExprSet expr_set;
      auto predicate = annotated_expr.GetExpr();
      parser::ExpressionUtil::GetTupleValueExprs(&expr_set, predicate);
      for (auto &col : expr_set) {
        AddBaseTableStats(col, table_stats, &predicate_stats);
      }
    }

    // Use predicates to estimate cardinality. If we were unable to find any column stats from the catalog, default to 0
    if (table_stats->GetColumnCount() == 0) {
      root_group->SetNumRows(0);
    } else {
      auto est = EstimateCardinalityForFilter(table_stats->GetNumRows(), predicate_stats, op->GetPredicates());
      root_group->SetNumRows(static_cast<int>(est));
    }
  }

  // Add the stats to the group
  for (auto &column_name_stats_pair : required_stats) {
    auto &column_name = column_name_stats_pair.first;
    auto &column_stats = column_name_stats_pair.second;
    column_stats->SetNumRows(root_group->GetNumRows());
    root_group->AddStats(column_name, std::move(column_stats));
  }
}

void StatsCalculator::Visit(UNUSED_ATTRIBUTE const LogicalQueryDerivedGet *op) {
  // TODO(boweic): Implement stats calculation for logical query derive get
  auto root_group = context_->GetMemo().GetGroupByID(gexpr_->GetGroupID());
  root_group->SetNumRows(0);
  for (auto &col : required_cols_) {
    NOISEPAGE_ASSERT(col->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE, "CVE expected");
    auto tv_expr = col.CastManagedPointerTo<parser::ColumnValueExpression>();
    root_group->AddStats(tv_expr->GetFullName(), CreateDefaultStats(tv_expr));
  }
}

void StatsCalculator::Visit(const LogicalInnerJoin *op) {
  // Check if there's join condition
  NOISEPAGE_ASSERT(gexpr_->GetChildrenGroupsSize() == 2, "Join must have two children");
  auto left_child_group = context_->GetMemo().GetGroupByID(gexpr_->GetChildGroupId(0));
  auto right_child_group = context_->GetMemo().GetGroupByID(gexpr_->GetChildGroupId(1));
  auto root_group = context_->GetMemo().GetGroupByID(gexpr_->GetGroupID());

  // Calculate output num rows first
  if (root_group->GetNumRows() == -1) {
    size_t curr_rows = left_child_group->GetNumRows() * right_child_group->GetNumRows();
    for (auto &annotated_expr : op->GetJoinPredicates()) {
      // See if there are join conditions
      if (annotated_expr.GetExpr()->GetExpressionType() == parser::ExpressionType::COMPARE_EQUAL &&
          annotated_expr.GetExpr()->GetChild(0)->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE &&
          annotated_expr.GetExpr()->GetChild(1)->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) {
        auto left_child = annotated_expr.GetExpr()->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
        auto right_child = annotated_expr.GetExpr()->GetChild(1).CastManagedPointerTo<parser::ColumnValueExpression>();
        auto left_col = left_child->GetFullName();
        auto right_col = right_child->GetFullName();
        if ((left_child_group->HasColumnStats(left_col) && right_child_group->HasColumnStats(right_col)) ||
            (left_child_group->HasColumnStats(right_col) && right_child_group->HasColumnStats(left_col))) {
          curr_rows /= std::max(std::max(left_child_group->GetNumRows(), right_child_group->GetNumRows()), 1);
        }
      }
    }
    root_group->SetNumRows(static_cast<int>(curr_rows));
  }

  size_t num_rows = root_group->GetNumRows();
  for (auto &col : required_cols_) {
    NOISEPAGE_ASSERT(col->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE, "CVE expected");
    auto tv_expr = col.CastManagedPointerTo<parser::ColumnValueExpression>();
    auto col_name = tv_expr->GetFullName();
    std::unique_ptr<ColumnStats> column_stats;

    // Make a copy from the child stats
    if (left_child_group->HasColumnStats(col_name)) {
      column_stats = std::make_unique<ColumnStats>(*left_child_group->GetStats(col_name));
    } else {
      NOISEPAGE_ASSERT(right_child_group->HasColumnStats(col_name), "Name must be in right group");
      column_stats = std::make_unique<ColumnStats>(*right_child_group->GetStats(col_name));
    }

    // Reset num_rows
    column_stats->SetNumRows(num_rows);
    root_group->AddStats(col_name, std::move(column_stats));
  }

  // TODO(boweic): calculate stats based on predicates other than join conditions
}

void StatsCalculator::Visit(UNUSED_ATTRIBUTE const LogicalAggregateAndGroupBy *op) {
  // TODO(boweic): For now we just pass the stats needed without any computation, need implement aggregate stats
  NOISEPAGE_ASSERT(gexpr_->GetChildrenGroupsSize() == 1, "Aggregate must have 1 child");

  // First, set num rows
  auto child_group = context_->GetMemo().GetGroupByID(gexpr_->GetChildGroupId(0));
  context_->GetMemo().GetGroupByID(gexpr_->GetGroupID())->SetNumRows(child_group->GetNumRows());
  for (auto &col : required_cols_) {
    NOISEPAGE_ASSERT(col->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE, "CVE expected");
    auto col_name = col.CastManagedPointerTo<parser::ColumnValueExpression>()->GetFullName();

    NOISEPAGE_ASSERT(child_group->HasColumnStats(col_name), "Stats missing in child group");
    context_->GetMemo()
        .GetGroupByID(gexpr_->GetGroupID())
        ->AddStats(col_name, std::make_unique<ColumnStats>(*child_group->GetStats(col_name)));
  }
}

void StatsCalculator::Visit(const LogicalLimit *op) {
  NOISEPAGE_ASSERT(gexpr_->GetChildrenGroupsSize() == 1, "Limit must have 1 child");
  auto child_group = context_->GetMemo().GetGroupByID(gexpr_->GetChildGroupId(0));
  auto group = context_->GetMemo().GetGroupByID(gexpr_->GetGroupID());
  group->SetNumRows(std::min(static_cast<int>(op->GetLimit()), child_group->GetNumRows()));
  for (auto &col : required_cols_) {
    NOISEPAGE_ASSERT(col->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE, "CVE expected");
    auto col_name = col.CastManagedPointerTo<parser::ColumnValueExpression>()->GetFullName();

    NOISEPAGE_ASSERT(child_group->HasColumnStats(col_name), "Stats missing in child group");
    auto stats = std::make_unique<ColumnStats>(*child_group->GetStats(col_name));
    stats->SetNumRows(group->GetNumRows());
    group->AddStats(col_name, std::move(stats));
  }
}

void StatsCalculator::AddBaseTableStats(common::ManagedPointer<parser::AbstractExpression> col,
                                        common::ManagedPointer<TableStats> table_stats,
                                        std::unordered_map<std::string, std::unique_ptr<ColumnStats>> *stats) {
  NOISEPAGE_ASSERT(col->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE, "Expected ColumnValue");
  auto tv_expr = col.CastManagedPointerTo<parser::ColumnValueExpression>();
  if (table_stats->GetColumnCount() == 0 || !table_stats->HasColumnStats(tv_expr->GetColumnOid())) {
    // We do not have stats for the table yet, use default value
    stats->insert(std::make_pair(tv_expr->GetFullName(), CreateDefaultStats(tv_expr)));
  } else {
    stats->insert(std::make_pair(tv_expr->GetFullName(),
                                 std::make_unique<ColumnStats>(*table_stats->GetColumnStats(tv_expr->GetColumnOid()))));
  }
}

size_t StatsCalculator::EstimateCardinalityForFilter(
    size_t num_rows, const std::unordered_map<std::string, std::unique_ptr<ColumnStats>> &predicate_stats,
    const std::vector<AnnotatedExpression> &predicates) {
  // First, construct the table stats as the interface needed it to compute selectivity
  // TODO(boweic): We may want to modify the interface of selectivity computation to not use table_stats
  std::vector<common::ManagedPointer<ColumnStats>> predicate_stats_vec;
  auto table_stats = new TableStats();
  for (auto &predicate : predicate_stats) {
    table_stats->AddColumnStats(std::make_unique<ColumnStats>(*predicate.second));
  }

  double selectivity = 1.F;
  for (auto &annotated_expr : predicates) {
    // Loop over conjunction exprs
    selectivity *=
        CalculateSelectivityForPredicate(common::ManagedPointer<TableStats>(table_stats), annotated_expr.GetExpr());
  }

  // Update selectivity
  return static_cast<size_t>(static_cast<double>(num_rows) * selectivity);
}

// Calculate the selectivity given the predicate and the stats of columns in the
// predicate
double StatsCalculator::CalculateSelectivityForPredicate(common::ManagedPointer<TableStats> predicate_table_stats,
                                                         common::ManagedPointer<parser::AbstractExpression> expr) {
  double selectivity = 1.F;
  if (predicate_table_stats->GetColumnCount() == 0) {
    return selectivity;
  }

  // Base case : Column Op Val
  if ((expr->GetChild(0)->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE &&
       (expr->GetChild(1)->GetExpressionType() == parser::ExpressionType::VALUE_CONSTANT ||
        expr->GetChild(1)->GetExpressionType() == parser::ExpressionType::VALUE_PARAMETER)) ||

      (expr->GetChild(1)->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE &&
       (expr->GetChild(0)->GetExpressionType() == parser::ExpressionType::VALUE_CONSTANT ||
        expr->GetChild(0)->GetExpressionType() == parser::ExpressionType::VALUE_PARAMETER))) {
    // For a [column (operator) value] or [value (operator) column] predicate,
    // left_expr gets a reference to the ColumnValueExpression
    // right_index is the child index to the value
    int right_index = expr->GetChild(0)->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE ? 1 : 0;
    auto left_expr = expr->GetChild(1 - right_index);
    NOISEPAGE_ASSERT(left_expr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE, "CVE expected");
    auto col_name = left_expr.CastManagedPointerTo<parser::ColumnValueExpression>()->GetFullName();

    auto expr_type = expr->GetExpressionType();
    if (right_index == 0) {
      expr_type = parser::ExpressionUtil::ReverseComparisonExpressionType(expr_type);
    }

    std::unique_ptr<parser::ConstantValueExpression> value;
    if (expr->GetChild(right_index)->GetExpressionType() == parser::ExpressionType::VALUE_CONSTANT) {
      auto cve = expr->GetChild(right_index).CastManagedPointerTo<parser::ConstantValueExpression>();
      value = std::unique_ptr<parser::ConstantValueExpression>{
          reinterpret_cast<parser::ConstantValueExpression *>(cve->Copy().release())};
    } else {
      auto pve = expr->GetChild(right_index).CastManagedPointerTo<parser::ParameterValueExpression>();
      value = std::make_unique<parser::ConstantValueExpression>(type::TypeId::PARAMETER_OFFSET,
                                                                execution::sql::Integer(pve->GetValueIdx()));
    }

    ValueCondition condition(col_name, expr_type, std::move(value));
    selectivity = Selectivity::ComputeSelectivity(predicate_table_stats, condition);
  } else if (expr->GetExpressionType() == parser::ExpressionType::CONJUNCTION_AND ||
             expr->GetExpressionType() == parser::ExpressionType::CONJUNCTION_OR) {
    double left_selectivity = CalculateSelectivityForPredicate(predicate_table_stats, expr->GetChild(0));
    double right_selectivity = CalculateSelectivityForPredicate(predicate_table_stats, expr->GetChild(1));
    if (expr->GetExpressionType() == parser::ExpressionType::CONJUNCTION_AND) {
      selectivity = left_selectivity * right_selectivity;
    } else {
      selectivity = left_selectivity + right_selectivity - left_selectivity * right_selectivity;
    }
  }

  return selectivity;
}

}  // namespace noisepage::optimizer
