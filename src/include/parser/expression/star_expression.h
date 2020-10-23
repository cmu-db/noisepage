#pragma once

#include <memory>
#include <vector>

#include "parser/expression/abstract_expression.h"

namespace noisepage::parser {
/**
 * StarExpression represents a star in expressions like COUNT(*).
 */
class StarExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new star expression, e.g. as in COUNT(*).
   */
  StarExpression() : AbstractExpression(ExpressionType::STAR, type::TypeId::INTEGER, {}) {}

  /**
   * Copies this StarExpression
   * @returns this
   */
  std::unique_ptr<AbstractExpression> Copy() const override;
  // TODO(Tianyu): This really should be a singleton object
  // ^WAN: jokes on you there's mutable state now and it can't be hahahaha

  /**
   * Creates a copy of the current AbstractExpression with new children implanted.
   * The children should not be owned by any other AbstractExpression.
   * @param children New children to be owned by the copy
   * @returns copy of this
   */
  std::unique_ptr<AbstractExpression> CopyWithChildren(
      std::vector<std::unique_ptr<AbstractExpression>> &&children) const override {
    NOISEPAGE_ASSERT(children.empty(), "StarExpression should have 0 children");
    return Copy();
  }

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override { v->Visit(common::ManagedPointer(this)); }
};

DEFINE_JSON_HEADER_DECLARATIONS(StarExpression);

}  // namespace noisepage::parser
