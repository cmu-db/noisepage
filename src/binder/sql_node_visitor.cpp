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
#include "parser/expression/type_cast_expression.h"

namespace terrier {
void binder::SqlNodeVisitor::Visit(common::ManagedPointer<parser::AggregateExpression> expr, common::ManagedPointer<binder::BinderSherpa> sherpa) {
  expr->AcceptChildren(common::ManagedPointer(this), sherpa);
}
void binder::SqlNodeVisitor::Visit(common::ManagedPointer<parser::CaseExpression> expr, common::ManagedPointer<binder::BinderSherpa> sherpa) {
  expr->AcceptChildren(common::ManagedPointer(this), sherpa);
}
void binder::SqlNodeVisitor::Visit(common::ManagedPointer<parser::ColumnValueExpression> expr, common::ManagedPointer<binder::BinderSherpa> sherpa) {
  expr->AcceptChildren(common::ManagedPointer(this), sherpa);
}
void binder::SqlNodeVisitor::Visit(common::ManagedPointer<parser::ComparisonExpression> expr, common::ManagedPointer<binder::BinderSherpa> sherpa) {
  expr->AcceptChildren(common::ManagedPointer(this), sherpa);
}
void binder::SqlNodeVisitor::Visit(common::ManagedPointer<parser::ConjunctionExpression> expr, common::ManagedPointer<binder::BinderSherpa> sherpa) {
  expr->AcceptChildren(common::ManagedPointer(this), sherpa);
}
void binder::SqlNodeVisitor::Visit(common::ManagedPointer<parser::ConstantValueExpression> expr, common::ManagedPointer<binder::BinderSherpa> sherpa) {
  expr->AcceptChildren(common::ManagedPointer(this), sherpa);
}
void binder::SqlNodeVisitor::Visit(common::ManagedPointer<parser::DefaultValueExpression> expr, common::ManagedPointer<binder::BinderSherpa> sherpa) {
  expr->AcceptChildren(common::ManagedPointer(this), sherpa);
}
void binder::SqlNodeVisitor::Visit(common::ManagedPointer<parser::DerivedValueExpression> expr, common::ManagedPointer<binder::BinderSherpa> sherpa) {
  expr->AcceptChildren(common::ManagedPointer(this), sherpa);
}
void binder::SqlNodeVisitor::Visit(common::ManagedPointer<parser::FunctionExpression> expr, common::ManagedPointer<binder::BinderSherpa> sherpa) {
  expr->AcceptChildren(common::ManagedPointer(this), sherpa);
}
void binder::SqlNodeVisitor::Visit(common::ManagedPointer<parser::OperatorExpression> expr, common::ManagedPointer<binder::BinderSherpa> sherpa) {
  expr->AcceptChildren(common::ManagedPointer(this), sherpa);
}
void binder::SqlNodeVisitor::Visit(common::ManagedPointer<parser::ParameterValueExpression> expr, common::ManagedPointer<binder::BinderSherpa> sherpa) {
  expr->AcceptChildren(common::ManagedPointer(this), sherpa);
}
void binder::SqlNodeVisitor::Visit(common::ManagedPointer<parser::StarExpression> expr, common::ManagedPointer<binder::BinderSherpa> sherpa) {
  expr->AcceptChildren(common::ManagedPointer(this), sherpa);
}
void binder::SqlNodeVisitor::Visit(common::ManagedPointer<parser::SubqueryExpression> expr, common::ManagedPointer<binder::BinderSherpa> sherpa) {
  expr->AcceptChildren(common::ManagedPointer(this), sherpa);
}
void binder::SqlNodeVisitor::Visit(common::ManagedPointer<parser::TypeCastExpression> expr, common::ManagedPointer<binder::BinderSherpa> sherpa) {
  expr->AcceptChildren(common::ManagedPointer(this), sherpa);
}

}  // namespace terrier
