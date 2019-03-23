#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "parser/expression_defs.h"
#include "type/transient_value.h"

namespace terrier::parser {

/**
 * Represents an operator.
 */
class OperatorExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new operator.
   * @param expression_type type of operator
   * @param return_value_type return type of the operator
   * @param children vector containing arguments to the operator left to right
   */
  OperatorExpression(const ExpressionType expression_type, const type::TypeId return_value_type,
                     std::vector<std::shared_ptr<AbstractExpression>> &&children)
      : AbstractExpression(expression_type, return_value_type, std::move(children)) {}

  std::unique_ptr<AbstractExpression> Copy() const override { return std::make_unique<OperatorExpression>(*this); }
};

}  // namespace terrier::parser
