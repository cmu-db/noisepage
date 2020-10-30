#include "parser/expression/type_cast_expression.h"

#include "binder/sql_node_visitor.h"
#include "common/json.h"

namespace noisepage::parser {

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

void TypeCastExpression::Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) {
  v->Visit(common::ManagedPointer(this));
}

DEFINE_JSON_BODY_DECLARATIONS(TypeCastExpression);

}  // namespace noisepage::parser
