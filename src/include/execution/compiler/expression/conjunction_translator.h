#pragma once
#include "execution/compiler/expression/expression_translator.h"

namespace tpl::compiler {

/**
 * Conjunction Translator
 */
class ConjunctionTranslator : public ExpressionTranslator {
 public:
  /**
   * Constructor
   * @param expression expression to translate
   * @param codegen code generator to use
   */
  ConjunctionTranslator(const terrier::parser::AbstractExpression *expression, CodeGen * codegen);

  ast::Expr *DeriveExpr(OperatorTranslator * translator) override;

 private:
  ExpressionTranslator * left_;
  ExpressionTranslator * right_;
};
}  // namespace tpl::compiler
