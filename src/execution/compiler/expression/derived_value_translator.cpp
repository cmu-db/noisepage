#include "execution/compiler/expression/derived_value_translator.h"

#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/work_context.h"
#include "parser/expression/derived_value_expression.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::execution::compiler {

DerivedValueTranslator::DerivedValueTranslator(const parser::DerivedValueExpression &expr,
                                               CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {}

ast::Expr *DerivedValueTranslator::DeriveValue(WorkContext *ctx, const ColumnValueProvider *provider) const {
  const auto &derived_expr = GetExpressionAs<parser::DerivedValueExpression>();
  return provider->GetChildOutput(ctx, derived_expr.GetTupleIdx(), derived_expr.GetValueIdx());
}

}  // namespace noisepage::execution::compiler
