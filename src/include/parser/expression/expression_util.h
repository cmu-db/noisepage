#pragma once

#include <algorithm>
#include <cstdlib>
#include <set>
#include <string>

#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "parser/expression/abstract_expression.h"
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
      ExpressionUtil::GetColumnOids(col_oids, common::ManagedPointer<AbstractExpression>(child.get()));
    }

    // If our mofo is a ColumnValueExpression, then pull out our
    // column ids
    auto etype = expr->GetExpressionType();
    if (etype == ExpressionType::COLUMN_VALUE) {
      auto t_expr = expr.CastManagedPointerTo<ColumnValueExpression>();
      col_oids->insert(t_expr->GetColumnOid());
    }
  }
};

}  // namespace terrier::parser::expression
