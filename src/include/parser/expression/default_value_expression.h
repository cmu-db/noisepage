#pragma once

#include <memory>
#include <vector>

#include "parser/expression/abstract_expression.h"

namespace terrier::parser {
/**
 * DefaultValueExpression represents a default value, e.g. in an INSERT.
 * Note that the return value type is unspecified and that the expression should be replaced by the binder.
 * TODO(WAN): check with Ling if this is happening. I believe we gave up on the binder translating to new objects.
 */
class DefaultValueExpression : public AbstractExpression {
 public:
  /** Instantiates a new default value expression. */
  DefaultValueExpression() : AbstractExpression(ExpressionType::VALUE_DEFAULT, type::TypeId::INVALID, {}) {}

  /**
   * Copies this DefaultValueExpression
   * @returns copy of this
   */
  std::unique_ptr<AbstractExpression> Copy() const override;

  /**
   * Copies this DefaultValueExpression with new children
   * @param children Children of new DefaultValueExpression
   * @returns copy of this with new children
   */
  std::unique_ptr<AbstractExpression> CopyWithChildren(
      std::vector<std::unique_ptr<AbstractExpression>> &&children) const override {
    TERRIER_ASSERT(children.empty(), "DefaultValueExpression should have 0 children");
    return Copy();
  }

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override { v->Visit(common::ManagedPointer(this)); }
};

DEFINE_JSON_HEADER_DECLARATIONS(DefaultValueExpression);

}  // namespace terrier::parser
