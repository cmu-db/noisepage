#pragma once

#include "execution/compiler/expression/expression_translator.h"
#include "execution/compiler/row_batch.h"
#include "parser/expression/operator_expression.h"

namespace tpl::compiler {

/**
 * Arithmetic Translator
 */
class ArithmeticTranslator : public ExpressionTranslator {
 public:
  ArithmeticTranslator(const terrier::parser::AbstractExpression *expression, CompilationContext *context);

  ast::Expr *DeriveExpr(const terrier::parser::AbstractExpression *expression, RowBatch *row) override;
};
}  // namespace tpl::compiler
