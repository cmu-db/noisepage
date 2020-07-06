#pragma once

#include <memory>

#include "execution/compiler/expression/expression_translator.h"

namespace terrier::parser {
class ParameterValueExpression;
}  // namespace terrier::parser

namespace terrier::execution::compiler {

/**
 * Parameter Value Translator
 */
class ParamValueTranslator : public ExpressionTranslator {
 public:
  /**
   * Create a translator for the given column value.
   * @param expr The expression to translate.
   * @param compilation_context The context in which translation occurs.
   */
  ParamValueTranslator(const parser::ParameterValueExpression &expr, CompilationContext *compilation_context);

  /**
   * Derive the value of the expression.
   * @param ctx The context containing collected subexpressions.
   * @param provider A provider for specific column values.
   * @return The value of the expression.
   */
  ast::Expr *DeriveValue(WorkContext *ctx, const ColumnValueProvider *provider) const override;
};
}  // namespace terrier::execution::compiler
