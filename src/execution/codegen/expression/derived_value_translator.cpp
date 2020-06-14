#include "execution/sql/codegen/expression/derived_value_translator.h"

#include "execution/sql/codegen/operators/operator_translator.h"
#include "execution/sql/codegen/work_context.h"
#include "execution/sql/planner/plannodes/abstract_plan_node.h"
#include "execution/sql/planner/plannodes/output_schema.h"

namespace terrier::execution::codegen {

DerivedValueTranslator::DerivedValueTranslator(const planner::DerivedValueExpression &expr,
                                               CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {}

ast::Expr *DerivedValueTranslator::DeriveValue(WorkContext *ctx, const ColumnValueProvider *provider) const {
  const auto &derived_expr = GetExpressionAs<planner::DerivedValueExpression>();
  return provider->GetChildOutput(ctx, derived_expr.GetTupleIdx(), derived_expr.GetValueIdx());
}

}  // namespace terrier::execution::codegen
