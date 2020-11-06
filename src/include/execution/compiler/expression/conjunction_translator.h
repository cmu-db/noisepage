#pragma once

#include "execution/compiler/expression/expression_translator.h"

namespace noisepage::parser {
class ConjunctionExpression;
}  // namespace noisepage::parser

namespace noisepage::execution::compiler {

/**
 * A translator for conjunction expressions.
 */
class ConjunctionTranslator : public ExpressionTranslator {
 public:
  /**
   * Create a translator for the given conjunction expression.
   * @param expr The expression to translate.
   * @param compilation_context The context in which translation occurs.
   */
  ConjunctionTranslator(const parser::ConjunctionExpression &expr, CompilationContext *compilation_context);

  /**
   * Derive the value of the expression.
   * @param ctx The context containing collected subexpressions.
   * @param provider A provider for specific column values.
   * @return The value of the expression.
   */
  ast::Expr *DeriveValue(WorkContext *ctx, const ColumnValueProvider *provider) const override;
};

}  // namespace noisepage::execution::compiler
