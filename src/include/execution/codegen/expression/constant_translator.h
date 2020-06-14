#pragma once

#include "execution/sql/codegen/expression/expression_translator.h"
#include "execution/sql/planner/expressions/constant_value_expression.h"

namespace terrier::execution::codegen {

/**
 * A translator for a constant value.
 */
class ConstantTranslator : public ExpressionTranslator {
 public:
  /**
   * Create a translator for the given constant value expression.
   * @param expr The expression to translate.
   * @param compilation_context The context in which translation occurs.
   */
  ConstantTranslator(const planner::ConstantValueExpression &expr, CompilationContext *compilation_context);

  /**
   * Derive the value of the expression.
   * @param ctx The context containing collected subexpressions.
   * @param provider A provider for specific column values.
   * @return The value of the expression.
   */
  ast::Expr *DeriveValue(WorkContext *ctx, const ColumnValueProvider *provider) const override;
};

}  // namespace terrier::execution::codegen
