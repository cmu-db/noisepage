#pragma once

#include <memory>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "parser/expression/expression_defs.h"
#include "type/type_id.h"

namespace terrier {
namespace parser {
namespace expression {

/**
 * Represents a star, e.g. COUNT(*).
 */
class StarExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new star expression, e.g. as in COUNT(*)
   */
  StarExpression()
      : AbstractExpression(ExpressionType::STAR, type::TypeId::INVALID,
                           std::vector<std::unique_ptr<AbstractExpression>>()) {}
};

}  // namespace expression
}  // namespace parser
}  // namespace terrier
