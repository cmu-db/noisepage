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
  TypeCastExpression(type::TypeId type, std::vector<std::shared_ptr<AbstractExpression>> &&children)
      : AbstractExpression(ExpressionType::OPERATOR_CAST, type, std::move(children)) {}

  /**
   * Default constructor for deserialization
   */
  TypeCastExpression() = default;

  std::shared_ptr<AbstractExpression> Copy() const override { return std::make_shared<TypeCastExpression>(*this); }

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }
};

DEFINE_JSON_DECLARATIONS(TypeCastExpression);

}  // namespace terrier::parser
