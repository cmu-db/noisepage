#pragma once

#include "execution/compiler/expression/expression_translator.h"

namespace noisepage::parser {
class OperatorExpression;
}  // namespace noisepage::parser

namespace noisepage::execution::compiler {

/**
 * A translator for null-checking expressions.
 */
class NullCheckTranslator : public ExpressionTranslator {
 public:
  /**
   * Create a translator for the given derived value.
   * @param expr The expression to translate.
   * @param compilation_context The context in which translation occurs.
   */
  NullCheckTranslator(const parser::OperatorExpression &expr, CompilationContext *compilation_context);

  /**
   * Derive the value of the expression.
   * @param ctx The context containing collected subexpressions.
   * @param provider A provider for specific column values.
   * @return The value of the expression.
   */
  ast::Expr *DeriveValue(WorkContext *ctx, const ColumnValueProvider *provider) const override;
};

}  // namespace noisepage::execution::compiler
