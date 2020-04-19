#include "parser/expression/default_value_expression.h"

namespace terrier::parser {

std::unique_ptr<AbstractExpression> DefaultValueExpression::Copy() const {
  auto expr = std::make_unique<DefaultValueExpression>();
  expr->SetMutableStateForCopy(*this);
  return expr;
}

}  // namespace terrier::parser