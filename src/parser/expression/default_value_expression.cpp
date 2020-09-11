#include "parser/expression/default_value_expression.h"

#include "common/json.h"

namespace terrier::parser {

std::unique_ptr<AbstractExpression> DefaultValueExpression::Copy() const {
  auto expr = std::make_unique<DefaultValueExpression>();
  expr->SetMutableStateForCopy(*this);
  return expr;
}

DEFINE_JSON_BODY_DECLARATIONS(DefaultValueExpression);

}  // namespace terrier::parser
