#pragma once

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
  StarExpression() : AbstractExpression(ExpressionType::STAR, type::TypeId::INVALID) {}
};

}  // namespace expression
}  // namespace parser
}  // namespace terrier
