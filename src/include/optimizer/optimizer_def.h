#pragma once

#include "common/macros.h"
#include "parser/expression_defs.h"
#include <string>
#include <unordered_set>

namespace terrier {

namespace parser {
class AbstractExpression;
}

namespace optimizer {
/**
 * Property types.
 */
enum class PropertyType : uint8_t {
  INVALID,
  COLUMNS,
  DISTINCT,
  SORT,
  LIMIT,
};

// Augment abstract expression with a table alias set
struct AnnotatedExpression {
  AnnotatedExpression(std::shared_ptr<parser::AbstractExpression> i_expr,
                      std::unordered_set<std::string> &i_set)
      : expr(i_expr), table_alias_set(i_set) {}
  AnnotatedExpression(const AnnotatedExpression &mt_expr)
      : expr(mt_expr.expr), table_alias_set(mt_expr.table_alias_set) {}
  std::shared_ptr<parser::AbstractExpression> expr;
  std::unordered_set<std::string> table_alias_set;
};

}  // namespace optimizer
}  // namespace terrier