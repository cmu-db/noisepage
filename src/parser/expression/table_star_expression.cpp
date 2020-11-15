#include "parser/expression/table_star_expression.h"

#include "binder/sql_node_visitor.h"
#include "common/json.h"

namespace noisepage::parser {

std::unique_ptr<AbstractExpression> TableStarExpression::Copy() const {
  auto expr = std::make_unique<TableStarExpression>();
  expr->SetMutableStateForCopy(*this);
  expr->target_table_specified_ = target_table_specified_;
  expr->target_table_ = target_table_;
  return expr;
}

void TableStarExpression::Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) {
  v->Visit(common::ManagedPointer(this));
}

DEFINE_JSON_BODY_DECLARATIONS(TableStarExpression);

}  // namespace noisepage::parser
