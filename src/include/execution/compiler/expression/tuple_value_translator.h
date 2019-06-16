#pragma once
#include "execution/compiler/expression/expression_translator.h"

namespace tpl::compiler {

/**
 * TupleValue Translator.
 */
class TupleValueTranslator : public ExpressionTranslator {
 public:
  /**
   * Constructor
   * @param expression expression to translate
   * @param codegen code generator to use
   */
  TupleValueTranslator(const terrier::parser::AbstractExpression *expression, CodeGen * codegen);

  ast::Expr *DeriveExpr(OperatorTranslator * translator) override;
};
}  // namespace tpl::compiler
