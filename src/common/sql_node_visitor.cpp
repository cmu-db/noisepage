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
void SqlNodeVisitor::Visit(parser::AggregateExpression *expr) { expr->AcceptChildren(this); }
void SqlNodeVisitor::Visit(parser::CaseExpression *expr) { expr->AcceptChildren(this); }
void SqlNodeVisitor::Visit(parser::ColumnValueExpression *expr) { expr->AcceptChildren(this); }
void SqlNodeVisitor::Visit(parser::ComparisonExpression *expr) { expr->AcceptChildren(this); }
void SqlNodeVisitor::Visit(parser::ConjunctionExpression *expr) { expr->AcceptChildren(this); }
void SqlNodeVisitor::Visit(parser::ConstantValueExpression *expr) { expr->AcceptChildren(this); }
void SqlNodeVisitor::Visit(parser::DefaultValueExpression *expr) { expr->AcceptChildren(this); }
void SqlNodeVisitor::Visit(parser::DerivedValueExpression *expr) { expr->AcceptChildren(this); }
void SqlNodeVisitor::Visit(parser::FunctionExpression *expr) { expr->AcceptChildren(this); }
void SqlNodeVisitor::Visit(parser::OperatorExpression *expr) { expr->AcceptChildren(this); }
void SqlNodeVisitor::Visit(parser::ParameterValueExpression *expr) { expr->AcceptChildren(this); }
void SqlNodeVisitor::Visit(parser::StarExpression *expr) { expr->AcceptChildren(this); }
void SqlNodeVisitor::Visit(parser::SubqueryExpression *expr) { expr->AcceptChildren(this); }
void SqlNodeVisitor::Visit(parser::TypeCastExpression *expr) { expr->AcceptChildren(this); }

}  // namespace terrier
