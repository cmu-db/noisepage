#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "parser/expression/abstract_expression.h"

namespace noisepage::parser {
/**
 * TypeCastExpression represents cast expressions of the form CAST(expr) or expr::TYPE.
 */
class TypeCastExpression : public AbstractExpression {
  // TODO(Ling):  Do we need a separate class for operator_cast? We can put it in operatorExpression
  // Wan: can you elaborate? How do you envision this being used?
 public:
  /** Instantiates a new type cast expression. */
  TypeCastExpression(type::TypeId type, std::vector<std::unique_ptr<AbstractExpression>> &&children)
      : AbstractExpression(ExpressionType::OPERATOR_CAST, type, std::move(children)) {}

  /** Default constructor for JSON deserialization. */
  TypeCastExpression() = default;

  /**
   * Copies this TypeCastExpression
   * @returns copy of this
   */
  std::unique_ptr<AbstractExpression> Copy() const override;
  /**
   * Creates a copy of the current AbstractExpression with new children implanted.
   * The children should not be owned by any other AbstractExpression.
   * @param children New children to be owned by the copy
   * @returns copy of this with new children
   */
  std::unique_ptr<AbstractExpression> CopyWithChildren(
      std::vector<std::unique_ptr<AbstractExpression>> &&children) const override;

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override { v->Visit(common::ManagedPointer(this)); }
};

DEFINE_JSON_HEADER_DECLARATIONS(TypeCastExpression);

}  // namespace noisepage::parser
