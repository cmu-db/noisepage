#include "parser/expression/type_cast_expression.h"

#include "common/json.h"

namespace terrier::parser {

std::unique_ptr<AbstractExpression> TypeCastExpression::Copy() const {
  std::vector<std::unique_ptr<AbstractExpression>> children;
  for (const auto &child : GetChildren()) {
    children.emplace_back(child->Copy());
  }
  return CopyWithChildren(std::move(children));
}

std::unique_ptr<AbstractExpression> TypeCastExpression::CopyWithChildren(
    std::vector<std::unique_ptr<AbstractExpression>> &&children) const {
  auto expr = std::make_unique<TypeCastExpression>(GetReturnValueType(), std::move(children));
  expr->SetMutableStateForCopy(*this);
  return expr;
}

DEFINE_JSON_BODY_DECLARATIONS(TypeCastExpression);

}  // namespace terrier::parser
