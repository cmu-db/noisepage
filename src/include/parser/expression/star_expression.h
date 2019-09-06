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
    return std::make_unique<StarExpression>();
  }

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }
};

DEFINE_JSON_DECLARATIONS(StarExpression);

}  // namespace terrier::parser
