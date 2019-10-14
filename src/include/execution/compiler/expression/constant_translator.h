#pragma once
#include "execution/compiler/expression/expression_translator.h"

namespace terrier::execution::compiler {

/**
 * Constant Translator
 */
class ConstantTranslator : public ExpressionTranslator {
 public:
  /**
   * Constructor
   * @param expression expression to translate
   * @param codegen code generator to use
   */
  ConstantTranslator(const terrier::parser::AbstractExpression *expression, CodeGen *codegen);

  ast::Expr *DeriveExpr(ExpressionEvaluator *evaluator) override;
};
}  // namespace terrier::execution::compiler
