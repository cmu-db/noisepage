#pragma once

#include <memory>
#include "parser/expression/abstract_expression.h"

namespace terrier::parser {

/**
 * Represents a default value, e.g. in an INSERT. Note that the return value type is unspecified,
 * this expression should be replaced by the binder.
 */
class DefaultValueExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new default value expression.
   */
  DefaultValueExpression() : AbstractExpression(ExpressionType::VALUE_DEFAULT, type::TypeId::INVALID, {}) {}

  std::shared_ptr<AbstractExpression> Copy() const override { return std::make_shared<DefaultValueExpression>(*this); }

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }
};

DEFINE_JSON_DECLARATIONS(DefaultValueExpression);

}  // namespace terrier::parser
