#include "common/sql_node_visitor.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/case_expression.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/comparison_expression.h"
#include "parser/expression/conjunction_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression/default_value_expression.h"
#include "parser/expression/derived_value_expression.h"
#include "parser/expression/function_expression.h"
#include "parser/expression/operator_expression.h"
#include "parser/expression/parameter_value_expression.h"
#include "parser/expression/star_expression.h"
#include "parser/expression/subquery_expression.h"
#include "parser/expression/type_cast_expression.h"

namespace terrier {
void SqlNodeVisitor::Visit(parser::AggregateExpression *expr, parser::ParseResult *parse_result) {
  expr->AcceptChildren(this, parse_result);
}
void SqlNodeVisitor::Visit(parser::CaseExpression *expr, parser::ParseResult *parse_result) {
  expr->AcceptChildren(this, parse_result);
}
void SqlNodeVisitor::Visit(parser::ColumnValueExpression *expr, parser::ParseResult *parse_result) {
  expr->AcceptChildren(this, parse_result);
}
void SqlNodeVisitor::Visit(parser::ComparisonExpression *expr, parser::ParseResult *parse_result) {
  expr->AcceptChildren(this, parse_result);
}
void SqlNodeVisitor::Visit(parser::ConjunctionExpression *expr, parser::ParseResult *parse_result) {
  expr->AcceptChildren(this, parse_result);
}
void SqlNodeVisitor::Visit(parser::ConstantValueExpression *expr, parser::ParseResult *parse_result) {
  expr->AcceptChildren(this, parse_result);
}
void SqlNodeVisitor::Visit(parser::DefaultValueExpression *expr, parser::ParseResult *parse_result) {
  expr->AcceptChildren(this, parse_result);
}
void SqlNodeVisitor::Visit(parser::DerivedValueExpression *expr, parser::ParseResult *parse_result) {
  expr->AcceptChildren(this, parse_result);
}
void SqlNodeVisitor::Visit(parser::FunctionExpression *expr, parser::ParseResult *parse_result) {
  expr->AcceptChildren(this, parse_result);
}
void SqlNodeVisitor::Visit(parser::OperatorExpression *expr, parser::ParseResult *parse_result) {
  expr->AcceptChildren(this, parse_result);
}
void SqlNodeVisitor::Visit(parser::ParameterValueExpression *expr, parser::ParseResult *parse_result) {
  expr->AcceptChildren(this, parse_result);
}
void SqlNodeVisitor::Visit(parser::StarExpression *expr, parser::ParseResult *parse_result) {
  expr->AcceptChildren(this, parse_result);
}
void SqlNodeVisitor::Visit(parser::SubqueryExpression *expr, parser::ParseResult *parse_result) {
  expr->AcceptChildren(this, parse_result);
}
void SqlNodeVisitor::Visit(parser::TypeCastExpression *expr, parser::ParseResult *parse_result) {
  expr->AcceptChildren(this, parse_result);
}

}  // namespace terrier
