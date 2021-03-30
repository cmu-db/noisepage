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
#include "optimizer/statistics/selectivity_util.h"
#include "optimizer/statistics/stats_storage.h"
#include "optimizer/statistics/table_stats.h"
#include "optimizer/statistics/value_condition.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression_util.h"

namespace noisepage::optimizer {

void StatsCalculator::CalculateStats(GroupExpression *gexpr, OptimizerContext *context) {
  gexpr_ = gexpr;
  context_ = context;
  gexpr->Contents()->Accept(common::ManagedPointer<OperatorVisitor>(this));
}

void StatsCalculator::Visit(const LogicalGet *op) {
  if (op->GetTableOid() == catalog::INVALID_TABLE_OID) {
    // Dummy scan
    return;
  }

  auto *root_group = context_->GetMemo().GetGroupByID(gexpr_->GetGroupID());

  // Compute selectivity at the first time
  if (root_group->GetNumRows() == -1) {
    const auto latched_table_stats_reference = context_->GetStatsStorage()->GetTableStats(
        op->GetDatabaseOid(), op->GetTableOid(), context_->GetCatalogAccessor());

    NOISEPAGE_ASSERT(latched_table_stats_reference.table_stats_.GetColumnCount() != 0,
                     "Should have table stats for all tables");
    // Use predicates to estimate cardinality.
    auto est = EstimateCardinalityForFilter(latched_table_stats_reference.table_stats_.GetNumRows(),
                                            latched_table_stats_reference.table_stats_, op->GetPredicates());
    root_group->SetNumRows(static_cast<int>(est));
  }
}

void StatsCalculator::Visit(UNUSED_ATTRIBUTE const LogicalQueryDerivedGet *op) {
  // TODO(boweic): Implement stats calculation for logical query derive get
  auto *root_group = context_->GetMemo().GetGroupByID(gexpr_->GetGroupID());
  root_group->SetNumRows(0);
}

void StatsCalculator::Visit(const LogicalInnerJoin *op) {
  // Check if there's join condition
  NOISEPAGE_ASSERT(gexpr_->GetChildrenGroupsSize() == 2, "Join must have two children");
  auto *left_child_group = context_->GetMemo().GetGroupByID(gexpr_->GetChildGroupId(0));
  auto *right_child_group = context_->GetMemo().GetGroupByID(gexpr_->GetChildGroupId(1));
  auto *root_group = context_->GetMemo().GetGroupByID(gexpr_->GetGroupID());

  // Calculate output num rows first
  if (root_group->GetNumRows() == -1) {
    size_t curr_rows = left_child_group->GetNumRows() * right_child_group->GetNumRows();
    for (const auto &annotated_expr : op->GetJoinPredicates()) {
      // See if there are join conditions
      if (annotated_expr.GetExpr()->GetExpressionType() == parser::ExpressionType::COMPARE_EQUAL &&
          annotated_expr.GetExpr()->GetChild(0)->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE &&
          annotated_expr.GetExpr()->GetChild(1)->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) {
        NOISEPAGE_ASSERT(left_child_group->HasNumRows() && right_child_group->HasNumRows(),
                         "Child groups should have their stats derived");
        /*
         * TODO(Joseph Koshakow) This isn't really that accurate, it doesn't take into account overlap of predicates
         *  i.e. if predicate 1 matches two rows and predicate 2 matches the same two rows, then predicate 2 will have
         *  no affect on the total row count but we will unnecessary lower the total row count.
         */
        curr_rows /= std::max(std::max(left_child_group->GetNumRows(), right_child_group->GetNumRows()), 1);
      }
    }
    root_group->SetNumRows(static_cast<int>(curr_rows));
  }

  // TODO(boweic): calculate stats based on predicates other than join conditions
}

void StatsCalculator::Visit(const LogicalSemiJoin *op) {
  // Check if there's join condition
  NOISEPAGE_ASSERT(gexpr_->GetChildrenGroupsSize() == 2, "Join must have two children");
  auto *left_child_group = context_->GetMemo().GetGroupByID(gexpr_->GetChildGroupId(0));
  auto *right_child_group = context_->GetMemo().GetGroupByID(gexpr_->GetChildGroupId(1));
  auto *root_group = context_->GetMemo().GetGroupByID(gexpr_->GetGroupID());

  // Calculate output num rows first
  if (root_group->GetNumRows() == -1) {
    size_t curr_rows = left_child_group->GetNumRows() * right_child_group->GetNumRows();
    for (const auto &annotated_expr : op->GetJoinPredicates()) {
      // See if there are join conditions
      if (annotated_expr.GetExpr()->GetExpressionType() == parser::ExpressionType::COMPARE_EQUAL &&
          annotated_expr.GetExpr()->GetChild(0)->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE &&
          annotated_expr.GetExpr()->GetChild(1)->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) {
        NOISEPAGE_ASSERT(left_child_group->HasNumRows() && right_child_group->HasNumRows(),
                         "Child groups should have their stats derived");
        /*
         * TODO(Joseph Koshakow) This isn't really that accurate, it doesn't take into account overlap of predicates
         *  i.e. if predicate 1 matches two rows and predicate 2 matches the same two rows, then predicate 2 will have
         *  no affect on the total row count but we will unnecessary lower the total row count.
         */
        curr_rows /= std::max(std::max(left_child_group->GetNumRows(), right_child_group->GetNumRows()), 1);
      }
    }
    root_group->SetNumRows(static_cast<int>(curr_rows));
  }
}

void StatsCalculator::Visit(UNUSED_ATTRIBUTE const LogicalAggregateAndGroupBy *op) {
  // TODO(boweic): For now we just pass the stats needed without any computation, need implement aggregate stats
  NOISEPAGE_ASSERT(gexpr_->GetChildrenGroupsSize() == 1, "Aggregate must have 1 child");

  // First, set num rows
  auto *child_group = context_->GetMemo().GetGroupByID(gexpr_->GetChildGroupId(0));
  context_->GetMemo().GetGroupByID(gexpr_->GetGroupID())->SetNumRows(child_group->GetNumRows());
}

void StatsCalculator::Visit(const LogicalLimit *op) {
  // TODO(Joe Koshakow) To be more accurate this should probably take into account the limit offset
  NOISEPAGE_ASSERT(gexpr_->GetChildrenGroupsSize() == 1, "Limit must have 1 child");
  auto *child_group = context_->GetMemo().GetGroupByID(gexpr_->GetChildGroupId(0));
  auto *group = context_->GetMemo().GetGroupByID(gexpr_->GetGroupID());
  group->SetNumRows(std::min(static_cast<int>(op->GetLimit()), child_group->GetNumRows()));
}

size_t StatsCalculator::EstimateCardinalityForFilter(size_t num_rows, const TableStats &predicate_stats,
                                                     const std::vector<AnnotatedExpression> &predicates) {
  double selectivity = 1.F;
  for (const auto &annotated_expr : predicates) {
    // Loop over conjunction exprs
    selectivity *= CalculateSelectivityForPredicate(predicate_stats, annotated_expr.GetExpr());
  }

  // Update selectivity
  return static_cast<size_t>(static_cast<double>(num_rows) * selectivity);
}

// Calculate the selectivity given the predicate and the stats of columns in the
// predicate
double StatsCalculator::CalculateSelectivityForPredicate(const TableStats &predicate_table_stats,
                                                         common::ManagedPointer<parser::AbstractExpression> expr) {
  double selectivity = 1.F;
  if (predicate_table_stats.GetColumnCount() == 0) {
    return selectivity;
  }

  if (expr->GetExpressionType() == parser::ExpressionType::OPERATOR_NOT) {
    selectivity = 1 - CalculateSelectivityForPredicate(predicate_table_stats, expr->GetChild(0));
  } else if (expr->GetChildrenSize() == 1 &&
             expr->GetChild(0)->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) {
    auto child_expr = expr->GetChild(0);
    auto col_name = child_expr.CastManagedPointerTo<parser::ColumnValueExpression>()->GetFullName();
    auto col_oid = child_expr.CastManagedPointerTo<parser::ColumnValueExpression>()->GetColumnOid();
    auto expr_type = expr->GetExpressionType();
    ValueCondition condition(col_oid, col_name, expr_type, nullptr);
    selectivity = SelectivityUtil::ComputeSelectivity(predicate_table_stats, condition);
  } else if ((expr->GetChild(0)->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE &&
              (expr->GetChild(1)->GetExpressionType() == parser::ExpressionType::VALUE_CONSTANT ||
               expr->GetChild(1)->GetExpressionType() == parser::ExpressionType::VALUE_PARAMETER)) ||

             (expr->GetChild(1)->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE &&
              (expr->GetChild(0)->GetExpressionType() == parser::ExpressionType::VALUE_CONSTANT ||
               expr->GetChild(0)->GetExpressionType() == parser::ExpressionType::VALUE_PARAMETER))) {
    // Base case : Column Op Val

    // For a [column (operator) value] or [value (operator) column] predicate,
    // left_expr gets a reference to the ColumnValueExpression
    // right_index is the child index to the value
    int right_index = expr->GetChild(0)->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE ? 1 : 0;
    auto left_expr = expr->GetChild(1 - right_index);
    NOISEPAGE_ASSERT(left_expr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE, "CVE expected");
    auto col_name = left_expr.CastManagedPointerTo<parser::ColumnValueExpression>()->GetFullName();
    auto col_oid = left_expr.CastManagedPointerTo<parser::ColumnValueExpression>()->GetColumnOid();

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
      NOISEPAGE_ASSERT(context_->GetParams() != nullptr, "Query expected to have parameters");
      NOISEPAGE_ASSERT(context_->GetParams()->size() > pve->GetValueIdx(), "Query expected to have enough parameters");
      value = std::unique_ptr<parser::ConstantValueExpression>{reinterpret_cast<parser::ConstantValueExpression *>(
          context_->GetParams()->at(pve->GetValueIdx()).Copy().release())};
    }

    ValueCondition condition(col_oid, col_name, expr_type, std::move(value));
    selectivity = SelectivityUtil::ComputeSelectivity(predicate_table_stats, condition);
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
