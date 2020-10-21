#include "parser/expression/table_star_expression.h"

#include "common/json.h"

namespace terrier::parser {

std::unique_ptr<AbstractExpression> TableStarExpression::Copy() const {
  auto expr = std::make_unique<TableStarExpression>();
  expr->SetMutableStateForCopy(*this);
  expr->target_table_specified_ = target_table_specified_;
  expr->target_table_ = target_table_;
  return expr;
}

DEFINE_JSON_BODY_DECLARATIONS(TableStarExpression);

}  // namespace terrier::parser
