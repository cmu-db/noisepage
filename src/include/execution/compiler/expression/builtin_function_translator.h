#pragma once

// TODO(WAN): ???
#if 0
#include "execution/compiler/expression/expression_translator.h"
#include "parser/expression/builtin_function_expression.h"

namespace terrier::execution::compiler {

/**
 * Translator for TPL builtins functions.
 */
class BuiltinFunctionTranslator : public ExpressionTranslator {
 public:
  /**
   * Create a translator for an expression.
   * @param expr The expression.
   * @param compilation_context The context the translation occurs in.
   */
  BuiltinFunctionTranslator(const parser::BuiltinFunctionExpression &expr, CompilationContext *compilation_context);

  /**
   * Derive the value of the expression.
   * @param ctx The context containing collected subexpressions.
   * @param provider A provider for specific column values.
   * @return The value of the expression.
   */
  ast::Expr *DeriveValue(WorkContext *ctx, const ColumnValueProvider *provider) const override;
};

}  // namespace terrier::execution::compiler
#endif
