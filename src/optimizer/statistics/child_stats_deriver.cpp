#include "optimizer/statistics/child_stats_deriver.h"

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "optimizer/logical_operators.h"
#include "optimizer/memo.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression_util.h"

namespace noisepage::optimizer {

std::vector<ExprSet> ChildStatsDeriver::DeriveInputStats(GroupExpression *gexpr, ExprSet required_cols, Memo *memo) {
  required_cols_ = std::move(required_cols);
  gexpr_ = gexpr;
  memo_ = memo;
  output_ = std::vector<ExprSet>(gexpr->GetChildrenGroupsSize(), ExprSet{});
  gexpr->Contents()->Accept(common::ManagedPointer<OperatorVisitor>(this));
  return std::move(output_);
}

// TODO(boweic): support stats derivation for derivedGet
void ChildStatsDeriver::Visit(UNUSED_ATTRIBUTE const LogicalQueryDerivedGet *op) {}

void ChildStatsDeriver::Visit(const LogicalInnerJoin *op) {
  PassDownRequiredCols();
  for (auto &annotated_expr : op->GetJoinPredicates()) {
    ExprSet expr_set;
    parser::ExpressionUtil::GetTupleValueExprs(&expr_set, annotated_expr.GetExpr());
    for (auto &col : expr_set) {
      PassDownColumn(col);
    }
  }
}

void ChildStatsDeriver::Visit(UNUSED_ATTRIBUTE const LogicalLeftJoin *op) {}
void ChildStatsDeriver::Visit(UNUSED_ATTRIBUTE const LogicalRightJoin *op) {}
void ChildStatsDeriver::Visit(UNUSED_ATTRIBUTE const LogicalOuterJoin *op) {}
void ChildStatsDeriver::Visit(UNUSED_ATTRIBUTE const LogicalSemiJoin *op) {}

// TODO(boweic): support stats of aggregation
void ChildStatsDeriver::Visit(UNUSED_ATTRIBUTE const LogicalAggregateAndGroupBy *op) { PassDownRequiredCols(); }

void ChildStatsDeriver::PassDownRequiredCols() {
  for (auto &col : required_cols_) {
    // For now we only consider stats of single column
    PassDownColumn(col);
  }
}

void ChildStatsDeriver::PassDownColumn(common::ManagedPointer<parser::AbstractExpression> col) {
  NOISEPAGE_ASSERT(col->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE, "ColumnValue expected");
  auto tv_expr = col.CastManagedPointerTo<parser::ColumnValueExpression>();
  for (size_t idx = 0; idx < gexpr_->GetChildrenGroupsSize(); ++idx) {
    auto child_group = memo_->GetGroupByID(gexpr_->GetChildGroupId(static_cast<int>(idx)));
    if ((child_group->GetTableAliases().count(tv_expr->GetTableName()) != 0U) &&
        // If we have not derived the column stats yet
        !child_group->HasColumnStats(tv_expr->GetFullName())) {
      output_[idx].insert(col);
      break;
    }
  }
}

}  // namespace noisepage::optimizer
