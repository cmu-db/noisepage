#include "parser/expression/conjunction_expression.h"

#include "binder/sql_node_visitor.h"
#include "common/json.h"

namespace noisepage::parser {

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

void ConjunctionExpression::Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) {
  v->Visit(common::ManagedPointer(this));
}

DEFINE_JSON_BODY_DECLARATIONS(ConjunctionExpression);

}  // namespace noisepage::parser
