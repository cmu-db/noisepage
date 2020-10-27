#include "parser/expression/comparison_expression.h"

#include "common/json.h"

namespace noisepage::parser {

std::unique_ptr<AbstractExpression> ComparisonExpression::Copy() const {
  std::vector<std::unique_ptr<AbstractExpression>> children;
  for (const auto &child : GetChildren()) {
    children.emplace_back(child->Copy());
  }
  return CopyWithChildren(std::move(children));
}

std::unique_ptr<AbstractExpression> ComparisonExpression::CopyWithChildren(
    std::vector<std::unique_ptr<AbstractExpression>> &&children) const {
  auto expr = std::make_unique<ComparisonExpression>(GetExpressionType(), std::move(children));
  expr->SetMutableStateForCopy(*this);
  return expr;
}

DEFINE_JSON_BODY_DECLARATIONS(ComparisonExpression);

}  // namespace noisepage::parser
