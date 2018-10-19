#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "parser/expression/expression_defs.h"
#include "type/value.h"

namespace terrier {
namespace parser {

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
                     std::vector<std::shared_ptr<AbstractExpression>> &&children)
      : AbstractExpression(expression_type, return_value_type, std::move(children)) {}

  std::unique_ptr<AbstractExpression> Copy() const override {
    return std::unique_ptr<AbstractExpression>(new OperatorExpression(*this));
  }
};

}  // namespace parser
}  // namespace terrier
