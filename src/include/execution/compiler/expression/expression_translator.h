#pragma once

#include <type_traits>

#include "common/macros.h"
#include "execution/ast/ast_fwd.h"
#include "execution/compiler/expression/column_value_provider.h"

namespace noisepage::parser {
class AbstractExpression;
}  // namespace noisepage::parser

namespace noisepage::execution::compiler {

class CodeGen;
class CompilationContext;
class WorkContext;
class Pipeline;

/**
 * Base class for expression translators.
 */
class ExpressionTranslator {
 public:
  /**
   * Create a translator for an expression.
   * @param expr The expression.
   * @param compilation_context The context the translation occurs in.
   */
  ExpressionTranslator(const parser::AbstractExpression &expr, CompilationContext *compilation_context);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(ExpressionTranslator);

  /**
   * Destructor.
   */
  virtual ~ExpressionTranslator() = default;

  /**
   * Derive the TPL value of the expression.
   * @param ctx The context the derivation of expression is occurring in.
   * @param provider A provider for specific column values.
   * @return The TPL value of the expression.
   */
  virtual ast::Expr *DeriveValue(WorkContext *ctx, const ColumnValueProvider *provider) const = 0;

  /**
   * @return The expression being translated.
   */
  const parser::AbstractExpression &GetExpression() const { return expr_; }

  /** @return A pointer to the execution context. */
  ast::Expr *GetExecutionContextPtr() const;

 protected:
  /** The expression for this translator as its concrete type. */
  template <typename T>
  const T &GetExpressionAs() const {
    static_assert(std::is_base_of_v<parser::AbstractExpression, T>, "Template type is not an expression");
    return static_cast<const T &>(expr_);
  }

  /** Return the code generation instance. */
  CodeGen *GetCodeGen() const;

 private:
  /** The expression that's to be translated. */
  const parser::AbstractExpression &expr_;
  /** The context the translation is a part of. */
  CompilationContext *compilation_context_;
};

}  // namespace noisepage::execution::compiler
