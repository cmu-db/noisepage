#pragma once

#include <memory>
#include <vector>
#include "parser/expression/abstract_expression.h"

namespace terrier::parser {
/**
 * Represents a star, e.g. COUNT(*).
 */
class StarExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new star expression, e.g. as in COUNT(*)
   */
  StarExpression() : AbstractExpression(ExpressionType::STAR, type::TypeId::INVALID, {}) {}

  std::shared_ptr<AbstractExpression> Copy() const override {
    // TODO(Tianyu): This really should be a singleton object
    return std::make_shared<StarExpression>(*this);
  }

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }
};

DEFINE_JSON_DECLARATIONS(StarExpression);

}  // namespace terrier::parser
