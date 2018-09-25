#pragma once

#include "type/expression/abstract_expression.h"
#include "type/expression/expression_defs.h"
#include "type/type_id.h"

namespace terrier {
namespace type {
namespace expression {

/**
 * Represents a star, e.g. COUNT(*).
 */
class StarExpression : public AbstractExpression {
 public:
  StarExpression() : AbstractExpression(ExpressionType::STAR, TypeId::INVALID) {}
};

}  // namespace expression
}  // namespace type
}  // namespace terrier
