#include <algorithm>
#include <cmath>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "optimizer/memo.h"
#include "optimizer/optimizer_metadata.h"
#include "optimizer/statistics/column_stats.h"
#include "optimizer/statistics/selectivity.h"
#include "optimizer/statistics/stats_calculator.h"
#include "optimizer/statistics/stats_storage.h"
#include "optimizer/statistics/table_stats.h"
#include "optimizer/statistics/value_condition.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression_util.h"
#include "type/transient_value_factory.h"

namespace terrier::optimizer {

void StatsCalculator::CalculateStats(GroupExpression *gexpr, ExprSet required_cols, OptimizerMetadata *metadata) {
  gexpr_ = gexpr;
  required_cols_ = required_cols;
  metadata_ = metadata;
  gexpr->Op().Accept(this);
}

void StatsCalculator::Visit(const LogicalGet *op) {
  if (op->GetTableOID() == catalog::INVALID_TABLE_OID) {
    // Dummy scan
    return;
  }

  auto table_stats = metadata_->GetStatsStorage()->GetTableStats(op->GetDatabaseOID(), op->GetTableOID());
  if (table_stats == nullptr) {
    // no table stats
    return;
  }

  // First, get the required stats of the base table
  std::unordered_map<std::string, std::unique_ptr<ColumnStats>> required_stats;
  for (auto &col : required_cols_) {
    // Make a copy for required stats since we may want to modify later
    AddBaseTableStats(col, table_stats, &required_stats);
  }

  // Compute selectivity at the first time
  auto root_group = metadata_->GetMemo().GetGroupByID(gexpr_->GetGroupID());
  if (root_group->GetNumRows() == -1) {
    std::unordered_map<std::string, std::unique_ptr<ColumnStats>> predicate_stats;
    for (auto &annotated_expr : op->GetPredicates()) {
      ExprSet expr_set;
      auto predicate = annotated_expr.GetExpr().Get();
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
  auto root_group = metadata_->GetMemo().GetGroupByID(gexpr_->GetGroupID());
  root_group->SetNumRows(0);
  for (auto &col : required_cols_) {
    TERRIER_ASSERT(col->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE, "CVE expected");
    auto tv_expr = reinterpret_cast<const parser::ColumnValueExpression *>(col);
    root_group->AddStats(tv_expr->GetFullName(), CreateDefaultStats(tv_expr));
  }
}

void StatsCalculator::Visit(const LogicalInnerJoin *op) {
  // Check if there's join condition
  TERRIER_ASSERT(gexpr_->GetChildrenGroupsSize() == 2, "Join must have two children");
  auto left_child_group = metadata_->GetMemo().GetGroupByID(gexpr_->GetChildGroupId(0));
  auto right_child_group = metadata_->GetMemo().GetGroupByID(gexpr_->GetChildGroupId(1));
  auto root_group = metadata_->GetMemo().GetGroupByID(gexpr_->GetGroupID());

  // Calculate output num rows first
  if (root_group->GetNumRows() == -1) {
    size_t curr_rows = left_child_group->GetNumRows() * right_child_group->GetNumRows();
    for (auto &annotated_expr : op->GetJoinPredicates()) {
      // See if there are join conditions
      if (annotated_expr.GetExpr()->GetExpressionType() == parser::ExpressionType::COMPARE_EQUAL &&
          annotated_expr.GetExpr()->GetChild(0)->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE &&
          annotated_expr.GetExpr()->GetChild(1)->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) {
        auto left_child =
            reinterpret_cast<const parser::ColumnValueExpression *>(annotated_expr.GetExpr()->GetChild(0).Get());
        auto right_child =
            reinterpret_cast<const parser::ColumnValueExpression *>(annotated_expr.GetExpr()->GetChild(1).Get());
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
    TERRIER_ASSERT(col->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE, "CVE expected");
    auto tv_expr = reinterpret_cast<const parser::ColumnValueExpression *>(col);
    auto col_name = tv_expr->GetFullName();
    std::unique_ptr<ColumnStats> column_stats;

    // Make a copy from the child stats
    if (left_child_group->HasColumnStats(col_name)) {
      column_stats = std::make_unique<ColumnStats>(*left_child_group->GetStats(col_name));
    } else {
      TERRIER_ASSERT(right_child_group->HasColumnStats(col_name), "Name must be in right group");
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
  TERRIER_ASSERT(gexpr_->GetChildrenGroupsSize() == 1, "Aggregate must have 1 child");

  // First, set num rows
  auto child_group = metadata_->GetMemo().GetGroupByID(gexpr_->GetChildGroupId(0));
  metadata_->GetMemo().GetGroupByID(gexpr_->GetGroupID())->SetNumRows(child_group->GetNumRows());
  for (auto &col : required_cols_) {
    TERRIER_ASSERT(col->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE, "CVE expected");
    auto col_name = reinterpret_cast<const parser::ColumnValueExpression *>(col)->GetFullName();

    TERRIER_ASSERT(child_group->HasColumnStats(col_name), "Stats missing in child group");
    metadata_->GetMemo()
        .GetGroupByID(gexpr_->GetGroupID())
        ->AddStats(col_name, std::make_unique<ColumnStats>(*child_group->GetStats(col_name)));
  }
}

void StatsCalculator::Visit(const LogicalLimit *op) {
  TERRIER_ASSERT(gexpr_->GetChildrenGroupsSize() == 1, "Limit must have 1 child");
  auto child_group = metadata_->GetMemo().GetGroupByID(gexpr_->GetChildGroupId(0));
  auto group = metadata_->GetMemo().GetGroupByID(gexpr_->GetGroupID());
  group->SetNumRows(std::min(static_cast<int>(op->GetLimit()), child_group->GetNumRows()));
  for (auto &col : required_cols_) {
    TERRIER_ASSERT(col->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE, "CVE expected");
    auto col_name = reinterpret_cast<const parser::ColumnValueExpression *>(col)->GetFullName();

    TERRIER_ASSERT(child_group->HasColumnStats(col_name), "Stats missing in child group");
    auto stats = std::make_unique<ColumnStats>(*child_group->GetStats(col_name));
    stats->SetNumRows(group->GetNumRows());
    group->AddStats(col_name, std::move(stats));
  }
}

void StatsCalculator::Visit(UNUSED_ATTRIBUTE const LogicalDistinct *op) {
  // TODO(boweic) calculate stats for distinct
  TERRIER_ASSERT(gexpr_->GetChildrenGroupsSize() == 1, "Distinct must have 1 child");
  auto child_group = metadata_->GetMemo().GetGroupByID(gexpr_->GetChildGroupId(0));
  auto group = metadata_->GetMemo().GetGroupByID(gexpr_->GetGroupID());
  group->SetNumRows(child_group->GetNumRows());
  for (auto &col : required_cols_) {
    TERRIER_ASSERT(col->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE, "CVE expected");
    auto col_name = reinterpret_cast<const parser::ColumnValueExpression *>(col)->GetFullName();

    TERRIER_ASSERT(child_group->HasColumnStats(col_name), "Stats missing in child group");
    group->AddStats(col_name, std::make_unique<ColumnStats>(*child_group->GetStats(col_name)));
  }
}

void StatsCalculator::AddBaseTableStats(const parser::AbstractExpression *col,
                                        common::ManagedPointer<TableStats> table_stats,
                                        std::unordered_map<std::string, std::unique_ptr<ColumnStats>> *stats) {
  TERRIER_ASSERT(col->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE, "Expected ColumnValue");
  auto tv_expr = reinterpret_cast<const parser::ColumnValueExpression *>(col);
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
double StatsCalculator::CalculateSelectivityForPredicate(
    common::ManagedPointer<TableStats> predicate_table_stats,
    common::ManagedPointer<const parser::AbstractExpression> expr) {
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
    int right_index = expr->GetChild(0)->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE ? 1 : 0;
    auto left_expr = expr->GetChild(1 - right_index);
    TERRIER_ASSERT(left_expr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE, "CVE expected");
    auto col_name = reinterpret_cast<const parser::ColumnValueExpression *>(left_expr.Get())->GetFullName();

    auto expr_type = expr->GetExpressionType();
    if (right_index == 0) {
      expr_type = parser::ExpressionUtil::ReverseComparisonExpressionType(expr_type);
    }

    std::shared_ptr<type::TransientValue> value;
    if (expr->GetChild(right_index)->GetExpressionType() == parser::ExpressionType::VALUE_CONSTANT) {
      auto cve = reinterpret_cast<const parser::ConstantValueExpression *>(expr->GetChild(right_index).Get());
      value = std::make_shared<type::TransientValue>(cve->GetValue());
    } else {
      auto pve = reinterpret_cast<const parser::ParameterValueExpression *>(expr->GetChild(right_index).Get());
      value =
          std::make_shared<type::TransientValue>(type::TransientValueFactory::GetParameterOffset(pve->GetValueIdx()));
    }

    ValueCondition condition(col_name, expr_type, value);
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

}  // namespace terrier::optimizer
