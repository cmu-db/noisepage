#include "execution/compiler/expression/column_value_translator.h"
#include "parser/expression/column_value_expression.h"
#include "execution/compiler/translator_factory.h"
#include "execution/compiler/operator/seq_scan_translator.h"


namespace terrier::execution::compiler {
ColumnValueTranslator::ColumnValueTranslator(const terrier::parser::AbstractExpression *expression,
                                           CodeGen * codegen)
    : ExpressionTranslator(expression, codegen) {}

ast::Expr *ColumnValueTranslator::DeriveExpr(OperatorTranslator * translator) {
  auto column_val = GetExpressionAs<parser::ColumnValueExpression>();
  auto seqscan_op = static_cast<SeqScanTranslator *>(translator);
  return seqscan_op->GetTableColumn(column_val->GetColumnOid());
}
};  // namespace terrier::execution::compiler
