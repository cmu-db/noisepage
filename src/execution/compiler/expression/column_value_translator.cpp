#include "execution/compiler/expression/column_value_translator.h"

#include "execution/compiler/work_context.h"
#include "parser/expression/column_value_expression.h"

namespace noisepage::execution::compiler {

ColumnValueTranslator::ColumnValueTranslator(const parser::ColumnValueExpression &expr,
                                             CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {}

ast::Expr *ColumnValueTranslator::DeriveValue(UNUSED_ATTRIBUTE WorkContext *ctx,
                                              const ColumnValueProvider *provider) const {
  const auto &col_expr = GetExpressionAs<const parser::ColumnValueExpression>();
  return provider->GetTableColumn(col_expr.GetColumnOid());
}

}  // namespace noisepage::execution::compiler
