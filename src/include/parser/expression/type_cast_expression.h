#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"

namespace terrier::parser {
/**
 * Represents a type cast expression.
 */
class TypeCastExpression : public AbstractExpression {
  // TODO(Ling):  Do we need a separate class for operator_cast? We can put it in operatorExpression
 public:
  /**
   * Instantiates a new type cast expression.
   */
  TypeCastExpression(type::TypeId type, std::vector<const AbstractExpression *> children)
      : AbstractExpression(ExpressionType::OPERATOR_CAST, type, std::move(children)) {}

  /**
   * Default constructor for deserialization
   */
  TypeCastExpression() = default;

  ~TypeCastExpression() override = default;

  /**
   * Copies this TypeCastExpression
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
    return new TypeCastExpression(*this, std::move(children));
  }

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

 private:
  /**
   * Copy constructor for TypeCastExpression
   * Relies on AbstractExpression copy constructor for base members
   * @param other Other TypeCastExpression to copy from
   * @param children new TypeCastExpression's children
   */
  TypeCastExpression(const TypeCastExpression &other, std::vector<const AbstractExpression *> &&children)
    : AbstractExpression(other) {
    children_ = children;
  }
};

DEFINE_JSON_DECLARATIONS(TypeCastExpression);

}  // namespace terrier::parser
