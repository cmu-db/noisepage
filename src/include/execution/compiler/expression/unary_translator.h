#pragma once
#include "execution/compiler/expression/expression_translator.h"

namespace tpl::compiler {

class UnaryTranslator : public ExpressionTranslator {

 public:
  UnaryTranslator(const terrier::parser::AbstractExpression *expression, CompilationContext &context);

  ast::Expr *DeriveExpr(const terrier::parser::AbstractExpression *expression, RowBatch &row) override;
};
}