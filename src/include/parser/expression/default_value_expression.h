#pragma once

#include <memory>
#include "parser/expression/abstract_expression.h"

namespace terrier::parser {
/**
 * DefaultValueExpression represents a default value, e.g. in an INSERT.
 * Note that the return value type is unspecified and that the expression should be replaced by the binder.
 * TODO(WAN): check with Ling if this is happening. I believe we gave up on the binder translating to new objects.
 */
class DefaultValueExpression : public AbstractExpression {
 public:
  /** Instantiates a new default value expression. */
  DefaultValueExpression() : AbstractExpression(ExpressionType::VALUE_DEFAULT, type::TypeId::INVALID, {}) {}

  std::unique_ptr<AbstractExpression> Copy() const override {
    auto expr = std::make_unique<DefaultValueExpression>();
    expr->SetMutableStateForCopy(*this);
    return expr;
  }

  void Accept(SqlNodeVisitor *v, ParseResult *parse_result) override { v->Visit(this, parse_result); }
};

DEFINE_JSON_DECLARATIONS(DefaultValueExpression);

}  // namespace terrier::parser
