#include "execution/codegen/expression/derived_value_translator.h"

#include "execution/codegen/operators/operator_translator.h"
#include "execution/codegen/work_context.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/output_schema.h"

namespace terrier::execution::codegen {

DerivedValueTranslator::DerivedValueTranslator(const parser::DerivedValueExpression &expr,
                                               CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {}

ast::Expr *DerivedValueTranslator::DeriveValue(WorkContext *ctx, const ColumnValueProvider *provider) const {
  const auto &derived_expr = GetExpressionAs<planner::DerivedValueExpression>();
  return provider->GetChildOutput(ctx, derived_expr.GetTupleIdx(), derived_expr.GetValueIdx());
}

}  // namespace terrier::execution::codegen
