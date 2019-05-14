#pragma once

#include "parser/expression/operator_expression.h"
#include "execution/compiler/expression/expression_translator.h"
#include "execution/compiler/row_batch.h"

namespace tpl::compiler {

class ArithmeticTranslator : public ExpressionTranslator {

 public:
  ArithmeticTranslator(const terrier::parser::AbstractExpression *expression, CompilationContext *context);

  ast::Expr *DeriveExpr(const terrier::parser::AbstractExpression *expression, RowBatch *row) override;
};
}