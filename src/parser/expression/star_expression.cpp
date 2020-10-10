#include "parser/expression/star_expression.h"

#include "common/json.h"

namespace terrier::parser {

std::unique_ptr<AbstractExpression> StarExpression::Copy() const {
  auto expr = std::make_unique<StarExpression>();
  expr->SetMutableStateForCopy(*this);
  return expr;
}

DEFINE_JSON_BODY_DECLARATIONS(StarExpression);

}  // namespace terrier::parser
