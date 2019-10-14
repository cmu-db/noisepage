#include "execution/compiler/expression/column_value_translator.h"
#include "execution/compiler/operator/seq_scan_translator.h"
#include "execution/compiler/translator_factory.h"
#include "parser/expression/column_value_expression.h"

namespace terrier::execution::compiler {
ColumnValueTranslator::ColumnValueTranslator(const terrier::parser::AbstractExpression *expression, CodeGen *codegen)
    : ExpressionTranslator(expression, codegen) {}

ast::Expr *ColumnValueTranslator::DeriveExpr(ExpressionEvaluator *evaluator) {
  auto column_val = GetExpressionAs<parser::ColumnValueExpression>();
  return evaluator->GetTableColumn(column_val->GetColumnOid());
}
};  // namespace terrier::execution::compiler
