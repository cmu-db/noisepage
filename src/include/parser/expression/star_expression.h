#pragma once

#include <memory>
#include <vector>
#include "parser/expression/abstract_expression.h"

namespace terrier::parser {
/**
 * StarExpression represents a star in expressions like COUNT(*).
 */
class StarExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new star expression, e.g. as in COUNT(*).
   */
  StarExpression() : AbstractExpression(ExpressionType::STAR, type::TypeId::INVALID, {}) {}

  std::unique_ptr<AbstractExpression> Copy() const override {
    // TODO(Tianyu): This really should be a singleton object
    // ^WAN: jokes on you there's mutable state now and it can't be hahahaha
    auto expr = std::make_unique<StarExpression>();
    expr->SetMutableStateForCopy(*this);
    return expr;
  }

  void Accept(SqlNodeVisitor *v, ParseResult *parse_result) override { v->Visit(this, parse_result); }
};

DEFINE_JSON_DECLARATIONS(StarExpression);

}  // namespace terrier::parser
