#pragma once

#include <algorithm>
#include <cstdlib>
#include <set>
#include <string>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/column_value_expression.h"

namespace terrier::parser::expression {

/**
 * An utility class for Expression objects
 */
class ExpressionUtil {
 public:
  // Static utility class
  ExpressionUtil() = delete;

  /**
   * Populate the given set with all of the column oids referenced
   * in this expression tree.
   * @param col_oids place to store column oids in the expression
   * @param expr to extract column oids from
   */
  static void GetColumnOids(std::set<catalog::col_oid_t> *col_oids, common::ManagedPointer<AbstractExpression> expr) {
    // Recurse into our children
    for (const auto &child : expr->GetChildren()) {
      ExpressionUtil::GetColumnOids(col_oids, child);
    }

    // If our mofo is a ColumnValueExpression, then pull out our column ids
    auto etype = expr->GetExpressionType();
    if (etype == ExpressionType::COLUMN_VALUE) {
      auto t_expr = expr.CastManagedPointerTo<ColumnValueExpression>();
      col_oids->insert(t_expr->GetColumnOid());
    }
  }

  /**
   * Check if the expression type is an aggregate expression type
   * @param type Expression type
   * @return True if the expression type is one of the aggregation types
   */
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
   * @param aggr_exprs List of generated aggregate expressions
   * @param expr The abstract expression tree to be traversed
   */
  static void GetAggregateExprs(std::vector<common::ManagedPointer<AggregateExpression>> *aggr_exprs,
                                common::ManagedPointer<AbstractExpression> expr) {
    if (ExpressionUtil::IsAggregateExpression(expr->GetExpressionType())) {
      auto aggr_expr = expr.CastManagedPointerTo<AggregateExpression>();
      aggr_exprs->push_back(aggr_expr);
    } else {
      for (const auto &child : expr->GetChildren()) GetAggregateExprs(aggr_exprs, child);
    }
  }
};

}  // namespace terrier::parser::expression
