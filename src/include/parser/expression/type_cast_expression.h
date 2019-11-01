#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"

namespace terrier::parser {
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

  std::unique_ptr<AbstractExpression> Copy() const override {
    std::vector<std::unique_ptr<AbstractExpression>> children;
    for (const auto &child : GetChildren()) {
      children.emplace_back(child->Copy());
    }
    auto expr = std::make_unique<TypeCastExpression>(GetReturnValueType(), std::move(children));
    expr->SetMutableStateForCopy(*this);
    return expr;
  }

  void Accept(SqlNodeVisitor *v, ParseResult *parse_result) override { v->Visit(this, parse_result); }
};

DEFINE_JSON_DECLARATIONS(TypeCastExpression);

}  // namespace terrier::parser
