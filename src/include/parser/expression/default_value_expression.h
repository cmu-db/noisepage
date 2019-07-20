#pragma once

#include <memory>
#include <vector>
#include "parser/expression/abstract_expression.h"

namespace terrier::parser {

/**
 * Represents a default value, e.g. in an INSERT. Note that the return value type is unspecified,
 * this expression should be replaced by the binder.
 */
class DefaultValueExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new default value expression.
   */
  DefaultValueExpression() : AbstractExpression(ExpressionType::VALUE_DEFAULT, type::TypeId::INVALID, {}) {}

  /**
   * Copies this DefaultValueExpression
   * @returns copy of this
   */
  const AbstractExpression* Copy() const override { return new DefaultValueExpression(*this); }

  /**
   * Copies this DefaultValueExpression with new children
   * @param children Children of new DefaultValueExpression
   * @returns copy of this with new children
   */
  const AbstractExpression* CopyWithChildren(std::vector<const AbstractExpression*> children) const override {
    TERRIER_ASSERT(children.empty(), "DefaultValueExpression should have 0 children");
    return Copy();
  }

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

 private:
  /**
   * Copy constructor for DefaultValueExpression
   * Relies on AbstractExpression copy constructor for base members
   * @param other Other DefaultValueExpression to copy
   */
  DefaultValueExpression(const DefaultValueExpression &other) = default;
};

DEFINE_JSON_DECLARATIONS(DefaultValueExpression);

}  // namespace terrier::parser
