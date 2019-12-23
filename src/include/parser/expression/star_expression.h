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

  /**
   * Copies this StarExpression
   * @returns this
   */
  std::unique_ptr<AbstractExpression> Copy() const override {
    // TODO(Tianyu): This really should be a singleton object
    // ^WAN: jokes on you there's mutable state now and it can't be hahahaha
    auto expr = std::make_unique<StarExpression>();
    expr->SetMutableStateForCopy(*this);
    return expr;
  }

  /**
   * Creates a copy of the current AbstractExpression with new children implanted.
   * The children should not be owned by any other AbstractExpression.
   * @param children New children to be owned by the copy
   * @returns copy of this
   */
  std::unique_ptr<AbstractExpression> CopyWithChildren(
      std::vector<std::unique_ptr<AbstractExpression>> &&children) const override {
    TERRIER_ASSERT(children.empty(), "StarExpression should have 0 children");
    return Copy();
  }

  void Accept(SqlNodeVisitor *v, ParseResult *parse_result) override { v->Visit(this, parse_result); }
};

DEFINE_JSON_DECLARATIONS(StarExpression);

}  // namespace terrier::parser
