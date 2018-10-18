#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "parser/expression/expression_defs.h"
#include "type/value.h"

namespace terrier {
namespace parser {
namespace expression {

/**
 * Represents an operator.
 */
class OperatorExpression : public AbstractExpression {
  /**
   * Instantiates a new unary operator.
   * @param expression_type type of operator
   * @param return_value_type return type of the operator
   * @param children vector containing arguments to the operator left to right
   */
  OperatorExpression(const ExpressionType expression_type, const type::TypeId return_value_type,
                     std::vector<std::unique_ptr<AbstractExpression>> *children)
      : AbstractExpression(expression_type, return_value_type, std::move(*children)) {}

  AbstractExpression *Copy() const override { return new OperatorExpression(*this); }
};

}  // namespace expression
}  // namespace parser
}  // namespace terrier
