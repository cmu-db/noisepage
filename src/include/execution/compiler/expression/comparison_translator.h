#pragma once
#include "execution/compiler/expression/expression_translator.h"

namespace tpl::compiler {

/**
 * Comparison Translator
 */
class ComparisonTranslator : public ExpressionTranslator {
 public:
  /**
   * Constructor
   * @param expression expression to translate
   * @param context compilation context to use
   */
  ComparisonTranslator(const terrier::parser::AbstractExpression *expression, CompilationContext *context);

  ast::Expr *DeriveExpr(const terrier::parser::AbstractExpression *expression, RowBatch *row) override;
};
}  // namespace tpl::compiler
