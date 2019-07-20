#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"

namespace terrier::parser {

/**
 * Represents a logical conjunction expression.
 */
class ConjunctionExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new conjunction expression.
   * @param cmp_type type of conjunction
   * @param children vector containing exactly two children, left then right
   */
  ConjunctionExpression(const ExpressionType cmp_type, std::vector<const AbstractExpression *> children)
      : AbstractExpression(cmp_type, type::TypeId::BOOLEAN, std::move(children)) {}

  /**
   * Default constructor for deserialization
   */
  ConjunctionExpression() = default;

  ~ConjunctionExpression() override = default;

  /**
   * Copies ConjunctionExpression
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
    return new ConjunctionExpression(*this, std::move(children));
  }

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

 private:
  /**
   * Copy constructor for ConjunctionExpression
   * Relies on AbstractExpression Copy constructor for base members
   * @param other Other ConjunctionExpression to copy from
   * @param children New ConjunctionExpression's children
   */
  ConjunctionExpression(const ConjunctionExpression &other, std::vector<const AbstractExpression *> &&children)
    : AbstractExpression(other) {
    children_ = children;
  }
};

DEFINE_JSON_DECLARATIONS(ConjunctionExpression);

}  // namespace terrier::parser
