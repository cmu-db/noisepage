#include "binder/sql_node_visitor.h"

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
#include "parser/expression/table_star_expression.h"
#include "parser/expression/type_cast_expression.h"

namespace noisepage {
void binder::SqlNodeVisitor::Visit(common::ManagedPointer<parser::AggregateExpression> expr) {
  expr->AcceptChildren(common::ManagedPointer(this));
}
void binder::SqlNodeVisitor::Visit(common::ManagedPointer<parser::CaseExpression> expr) {
  expr->AcceptChildren(common::ManagedPointer(this));
}
void binder::SqlNodeVisitor::Visit(common::ManagedPointer<parser::ColumnValueExpression> expr) {
  expr->AcceptChildren(common::ManagedPointer(this));
}
void binder::SqlNodeVisitor::Visit(common::ManagedPointer<parser::ComparisonExpression> expr) {
  expr->AcceptChildren(common::ManagedPointer(this));
}
void binder::SqlNodeVisitor::Visit(common::ManagedPointer<parser::ConjunctionExpression> expr) {
  expr->AcceptChildren(common::ManagedPointer(this));
}
void binder::SqlNodeVisitor::Visit(common::ManagedPointer<parser::ConstantValueExpression> expr) {
  expr->AcceptChildren(common::ManagedPointer(this));
}
void binder::SqlNodeVisitor::Visit(common::ManagedPointer<parser::DefaultValueExpression> expr) {
  expr->AcceptChildren(common::ManagedPointer(this));
}
void binder::SqlNodeVisitor::Visit(common::ManagedPointer<parser::DerivedValueExpression> expr) {
  expr->AcceptChildren(common::ManagedPointer(this));
}
void binder::SqlNodeVisitor::Visit(common::ManagedPointer<parser::FunctionExpression> expr) {
  expr->AcceptChildren(common::ManagedPointer(this));
}
void binder::SqlNodeVisitor::Visit(common::ManagedPointer<parser::OperatorExpression> expr) {
  expr->AcceptChildren(common::ManagedPointer(this));
}
void binder::SqlNodeVisitor::Visit(common::ManagedPointer<parser::ParameterValueExpression> expr) {
  expr->AcceptChildren(common::ManagedPointer(this));
}
void binder::SqlNodeVisitor::Visit(common::ManagedPointer<parser::StarExpression> expr) {
  expr->AcceptChildren(common::ManagedPointer(this));
}
void binder::SqlNodeVisitor::Visit(common::ManagedPointer<parser::TableStarExpression> expr) {
  expr->AcceptChildren(common::ManagedPointer(this));
}
void binder::SqlNodeVisitor::Visit(common::ManagedPointer<parser::SubqueryExpression> expr) {
  expr->AcceptChildren(common::ManagedPointer(this));
}
void binder::SqlNodeVisitor::Visit(common::ManagedPointer<parser::TypeCastExpression> expr) {
  expr->AcceptChildren(common::ManagedPointer(this));
}

}  // namespace noisepage
