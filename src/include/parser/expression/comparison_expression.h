#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"

namespace terrier::parser {

/**
 * Represents a logical comparison expression.
 */
class ComparisonExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new comparison expression.
   * @param cmp_type type of comparison
   * @param children vector containing exactly two children, left then right
   */
  ComparisonExpression(const ExpressionType cmp_type, std::vector<const AbstractExpression *> children)
      : AbstractExpression(cmp_type, type::TypeId::BOOLEAN, std::move(children)) {}

  /**
   * Default constructor for deserialization
   */
  ComparisonExpression() = default;

  ~ComparisonExpression() override = default;

  /**
   * Copies the ComparisonExpression
   * @returns copy of this
   */
  const AbstractExpression *Copy() const override {
    std::vector<const AbstractExpression *> children;
    for (const auto *child : children_) {
      children.emplace_back(child->Copy());
    }
    return CopyWithChildren(std::move(children));
  }

  /**
   * Creates a copy of the current AbstractExpression with new children implanted.
   * The children should not be owned by any other AbstractExpression.
   * @param children New children to be owned by the copy
   * @returns copy of this with new children
   */
  const AbstractExpression *CopyWithChildren(std::vector<const AbstractExpression *> children) const override {
    return new ComparisonExpression(*this, std::move(children));
  }

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

 private:
  /**
   * Copy constructor for ComparisonExpression
   * Relies on AbstractExpression copy constructor for base members
   * @param other ComparisonExpression to copy from
   * @param children Children of the new ComparisonExpression
   */
  ComparisonExpression(const ComparisonExpression &other, std::vector<const AbstractExpression*> &&children)
    : AbstractExpression(other) {
    children_ = children;
  }
};

DEFINE_JSON_DECLARATIONS(ComparisonExpression);

}  // namespace terrier::parser
