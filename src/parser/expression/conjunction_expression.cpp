#include "parser/expression/conjunction_expression.h"

#include "common/json.h"

namespace terrier::parser {

std::unique_ptr<AbstractExpression> ConjunctionExpression::Copy() const {
  std::vector<std::unique_ptr<AbstractExpression>> children;
  for (const auto &child : GetChildren()) {
    children.emplace_back(child->Copy());
  }
  return CopyWithChildren(std::move(children));
}

std::unique_ptr<AbstractExpression> ConjunctionExpression::CopyWithChildren(
    std::vector<std::unique_ptr<AbstractExpression>> &&children) const {
  auto expr = std::make_unique<ConjunctionExpression>(GetExpressionType(), std::move(children));
  expr->SetMutableStateForCopy(*this);
  return expr;
}

DEFINE_JSON_BODY_DECLARATIONS(ConjunctionExpression);

}  // namespace terrier::parser
