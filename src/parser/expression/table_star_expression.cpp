#include "parser/expression/table_star_expression.h"

#include "common/json.h"

namespace terrier::parser {

std::unique_ptr<AbstractExpression> TableStarExpression::Copy() const {
  auto expr = std::make_unique<TableStarExpression>();
  expr->SetMutableStateForCopy(*this);
  return expr;
}

DEFINE_JSON_BODY_DECLARATIONS(TableStarExpression);

}  // namespace terrier::parser
