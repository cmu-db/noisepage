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
  PARSER_LOG_INFO("wtf1");
  expr->AcceptChildren(this, parse_result);
}
void SqlNodeVisitor::Visit(parser::CaseExpression *expr, parser::ParseResult *parse_result) {
  PARSER_LOG_INFO("wtf2");
  expr->AcceptChildren(this, parse_result);
}
void SqlNodeVisitor::Visit(parser::ColumnValueExpression *expr, parser::ParseResult *parse_result) {
  PARSER_LOG_INFO("wtf3");
  expr->AcceptChildren(this, parse_result);
}
void SqlNodeVisitor::Visit(parser::ComparisonExpression *expr, parser::ParseResult *parse_result) {
  PARSER_LOG_INFO("wtf4");
  expr->AcceptChildren(this, parse_result);
  PARSER_LOG_INFO("wtf4_done");
}
void SqlNodeVisitor::Visit(parser::ConjunctionExpression *expr, parser::ParseResult *parse_result) {
  PARSER_LOG_INFO("wtf5");
  expr->AcceptChildren(this, parse_result);
}
void SqlNodeVisitor::Visit(parser::ConstantValueExpression *expr, parser::ParseResult *parse_result) {
  PARSER_LOG_INFO("wtf6");
  expr->AcceptChildren(this, parse_result);
}
void SqlNodeVisitor::Visit(parser::DefaultValueExpression *expr, parser::ParseResult *parse_result) {
  PARSER_LOG_INFO("wtf7");
  expr->AcceptChildren(this, parse_result);
}
void SqlNodeVisitor::Visit(parser::DerivedValueExpression *expr, parser::ParseResult *parse_result) {
  PARSER_LOG_INFO("wtf8");
  expr->AcceptChildren(this, parse_result);
}
void SqlNodeVisitor::Visit(parser::FunctionExpression *expr, parser::ParseResult *parse_result) {
  PARSER_LOG_INFO("wtf9");
  expr->AcceptChildren(this, parse_result);
}
void SqlNodeVisitor::Visit(parser::OperatorExpression *expr, parser::ParseResult *parse_result) {
  PARSER_LOG_INFO("wtf10");
  expr->AcceptChildren(this, parse_result);
}
void SqlNodeVisitor::Visit(parser::ParameterValueExpression *expr, parser::ParseResult *parse_result) {
  PARSER_LOG_INFO("wtf11");
  expr->AcceptChildren(this, parse_result);
}
void SqlNodeVisitor::Visit(parser::StarExpression *expr, parser::ParseResult *parse_result) {
  PARSER_LOG_INFO("wtf12");
  expr->AcceptChildren(this, parse_result);
}
void SqlNodeVisitor::Visit(parser::SubqueryExpression *expr, parser::ParseResult *parse_result) {
  PARSER_LOG_INFO("wtf13");
  expr->AcceptChildren(this, parse_result);
}
void SqlNodeVisitor::Visit(parser::TypeCastExpression *expr, parser::ParseResult *parse_result) {
  PARSER_LOG_INFO("wtf14");
  expr->AcceptChildren(this, parse_result);
}

}  // namespace terrier
