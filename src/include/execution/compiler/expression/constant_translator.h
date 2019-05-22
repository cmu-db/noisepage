#pragma once
#include "execution/compiler/expression/expression_translator.h"

namespace tpl::compiler {
class ConstantTranslator : public ExpressionTranslator {
 public:
  ConstantTranslator(const terrier::parser::AbstractExpression *expression, CompilationContext *context);

  ast::Expr *DeriveExpr(const terrier::parser::AbstractExpression *expression, RowBatch *row) override;
};
}  // namespace tpl::compiler
