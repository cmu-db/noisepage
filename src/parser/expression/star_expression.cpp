#include "parser/expression/star_expression.h"

namespace terrier::parser {

std::unique_ptr<AbstractExpression> StarExpression::Copy() const {
  auto expr = std::make_unique<StarExpression>();
  expr->SetMutableStateForCopy(*this);
  return expr;
}

}  // namespace terrier::parser