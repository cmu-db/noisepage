#pragma once
#include <memory>
#include "execution/compiler/expression/expression_translator.h"

namespace terrier::execution::compiler {

/**
 * ColumnValue Translator.
 */
class ColumnValueTranslator : public ExpressionTranslator {
 public:
  /**
   * Constructor
   * @param expression expression to translate
   * @param codegen code generator to use
   */
  ColumnValueTranslator(const terrier::parser::AbstractExpression *expression, CodeGen *codegen);

  ast::Expr *DeriveExpr(ExpressionEvaluator *evaluator) override;
};
}  // namespace terrier::execution::compiler
