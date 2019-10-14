#pragma once
#include "execution/compiler/expression/expression_translator.h"

namespace terrier::execution::compiler {

/**
 * Comparison Translator
 */
class ComparisonTranslator : public ExpressionTranslator {
 public:
  /**
   * Constructor
   * @param expression expression to translate
   * @param codegen code generator to use
   */
  ComparisonTranslator(const terrier::parser::AbstractExpression *expression, CodeGen *codegen);

  ast::Expr *DeriveExpr(ExpressionEvaluator *evaluator) override;

 private:
  std::unique_ptr<ExpressionTranslator> left_;
  std::unique_ptr<ExpressionTranslator> right_;
};
}  // namespace terrier::execution::compiler
