#include "parser/sql_node_visitor.h"
#include "parser/expression/comparison_expression.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/conjunction_expression.h"
#include "parser/expression/function_expression.h"
#include "parser/expression/operator_expression.h"
#include "parser/expression/parameter_value_expression.h"
#include "parser/expression/star_expression.h"
#include "parser/expression/tuple_value_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression/case_expression.h"
#include "parser/expression/subquery_expression.h"

namespace terrier::parser {
std::vector<std::shared_ptr<sql::SqlAbstractExpression>> SqlNodeVisitor::Visit(ComparisonExpression *expr) {
  return expr->AcceptChildren(this);
}
std::vector<std::shared_ptr<sql::SqlAbstractExpression>> SqlNodeVisitor::Visit(AggregateExpression *expr) {
  return expr->AcceptChildren(this);
}
std::vector<std::shared_ptr<sql::SqlAbstractExpression>> SqlNodeVisitor::Visit(CaseExpression *expr) {
  return expr->AcceptChildren(this);
}
std::vector<std::shared_ptr<sql::SqlAbstractExpression>> SqlNodeVisitor::Visit(ConjunctionExpression *expr) {
  return expr->AcceptChildren(this);
}
std::vector<std::shared_ptr<sql::SqlAbstractExpression>> SqlNodeVisitor::Visit(ConstantValueExpression *expr) {
  return expr->AcceptChildren(this);
}
std::vector<std::shared_ptr<sql::SqlAbstractExpression>> SqlNodeVisitor::Visit(FunctionExpression *expr) {
  return expr->AcceptChildren(this);
}
std::vector<std::shared_ptr<sql::SqlAbstractExpression>> SqlNodeVisitor::Visit(OperatorExpression *expr) {
  return expr->AcceptChildren(this);
}
std::vector<std::shared_ptr<sql::SqlAbstractExpression>> SqlNodeVisitor::Visit(ParameterValueExpression *expr) {
  return expr->AcceptChildren(this);
}
std::vector<std::shared_ptr<sql::SqlAbstractExpression>> SqlNodeVisitor::Visit(StarExpression *expr) {
  return expr->AcceptChildren(this);
}
std::vector<std::shared_ptr<sql::SqlAbstractExpression>> SqlNodeVisitor::Visit(TupleValueExpression *expr) {
  return expr->AcceptChildren(this);
}
std::vector<std::shared_ptr<sql::SqlAbstractExpression>> SqlNodeVisitor::Visit(SubqueryExpression *expr) {
  return expr->AcceptChildren(this);
}

}  // namespace terrier::parser