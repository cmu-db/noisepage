#pragma once
#include "execution/compiler/expression/expression_translator.h"

namespace terrier::execution::compiler {

/**
 * Null Check Translator
 */
class NullCheckTranslator : public ExpressionTranslator {
 public:
  /**
   * Constructor
   * @param expression expression to translate
   * @param codegen code generator to use
   */
  NullCheckTranslator(const terrier::parser::AbstractExpression *expression, CodeGen *codegen);

  ast::Expr *DeriveExpr(ExpressionEvaluator *evaluator) override;

 private:
  std::unique_ptr<ExpressionTranslator> child_;
};
}  // namespace terrier::execution::compiler
