#pragma once

#include <string>
#include <vector>

#include "parser/expression/aggregate_expression.h"
#include "parser/expression/column_value_expression.h"

namespace terrier::parser {

class ExpressionUtil {
 public:
  static bool IsAggregateExpression(ExpressionType type) {
    switch (type) {
      case ExpressionType::AGGREGATE_COUNT:
      case ExpressionType::AGGREGATE_SUM:
      case ExpressionType::AGGREGATE_MIN:
      case ExpressionType::AGGREGATE_MAX:
      case ExpressionType::AGGREGATE_AVG:
        return true;
      default:
        return false;
    }
  }
  /**
   * Walks an expression trees and find all AggregationExprs subtrees.
   */
  static void GetAggregateExprs(std::vector<common::ManagedPointer<AggregateExpression>> *aggr_exprs,
                                common::ManagedPointer<AbstractExpression> expr) {
    std::vector<common::ManagedPointer<ColumnValueExpression>> dummy_col_exprs;
    GetAggregateExprs(aggr_exprs, &dummy_col_exprs, expr);
  }

  /**
   * Walks an expression trees and find all AggregationExprs and ColumnValueExprs subtrees.
   */
  static void GetAggregateExprs(std::vector<common::ManagedPointer<AggregateExpression>> *aggr_exprs,
                                std::vector<common::ManagedPointer<ColumnValueExpression>> *tv_exprs,
                                common::ManagedPointer<AbstractExpression> expr) {
    if (IsAggregateExpression(expr->GetExpressionType())) {
      auto aggr_expr = expr.CastManagedPointerTo<AggregateExpression>();
      aggr_exprs->push_back(aggr_expr);
    } else if (expr->GetExpressionType() == ExpressionType::COLUMN_VALUE) {
      tv_exprs->push_back(expr.CastManagedPointerTo<ColumnValueExpression>());
    } else {
      for (const auto &child : expr->GetChildren()) GetAggregateExprs(aggr_exprs, tv_exprs, child);
    }
  }
};
}  // namespace terrier::parser
