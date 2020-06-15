#include "execution/codegen/expression/column_value_translator.h"

#include "execution/codegen/work_context.h"

namespace terrier::execution::codegen {

ColumnValueTranslator::ColumnValueTranslator(const parser::ColumnValueExpression &expr,
                                             CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {}

ast::Expr *ColumnValueTranslator::DeriveValue(UNUSED_ATTRIBUTE WorkContext *ctx,
                                              const ColumnValueProvider *provider) const {
  auto &col_expr = GetExpressionAs<const parser::ColumnValueExpression>();
  return provider->GetTableColumn(col_expr.GetColumnOid());
}

}  // namespace terrier::execution::codegen
