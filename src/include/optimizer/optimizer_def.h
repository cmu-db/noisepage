#pragma once

#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include "common/macros.h"
#include "parser/expression_defs.h"

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
  AnnotatedExpression(std::shared_ptr<parser::AbstractExpression> i_expr, std::unordered_set<std::string> &&i_set)
      : expr(std::move(i_expr)), table_alias_set(std::move(i_set)) {}
  AnnotatedExpression(const AnnotatedExpression &mt_expr) = default;
  std::shared_ptr<parser::AbstractExpression> expr;
  std::unordered_set<std::string> table_alias_set;
};

}  // namespace optimizer
}  // namespace terrier
