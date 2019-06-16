#pragma once
#include "execution/compiler/expression/expression_translator.h"

namespace tpl::compiler {

/**
 * Unary Translator
 */
class UnaryTranslator : public ExpressionTranslator {
 public:
  /**
   * Constructor
   * @param expression expression to translate
   * @param codegen code generator to use
   */
  UnaryTranslator(const terrier::parser::AbstractExpression *expression, CodeGen * codegen);

  ast::Expr *DeriveExpr(OperatorTranslator * translator) override;
 private:
  ExpressionTranslator * child_;
};
}  // namespace tpl::compiler
