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

  ~StarExpression() override = default;

  /**
   * Copies this StarExpression
   * @returns this
   */
  const AbstractExpression *Copy() const override {
    // TODO(Tianyu): This really should be a singleton object
    return new StarExpression(*this);
  }

  /**
   * Creates a copy of the current AbstractExpression with new children implanted.
   * The children should not be owned by any other AbstractExpression.
   * @param children New children to be owned by the copy
   * @returns copy of this
   */
  const AbstractExpression *CopyWithChildren(std::vector<const AbstractExpression *> children) const override {
    TERRIER_ASSERT(children.empty(), "StarExpression should have 0 children");
    return Copy();
  }

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

 private:
  /**
   * Copy constructor for StarExpression
   * Relies on AbstractExpression copy constructor for base members
   * @param other Other AbstractExpression to copy
   */
  StarExpression(const StarExpression &other) = default;
};

DEFINE_JSON_DECLARATIONS(StarExpression);

}  // namespace terrier::parser
