#pragma once

#include "common/macros.h"
#include "execution/ast/ast.h"
#include "execution/compiler/compiler_defs.h"
#include "parser/expression/abstract_expression.h"

namespace tpl::compiler {
class CompilationContext;

/**
 * Represents a single tuple.
 * This gets passed into nodes and expressions, which then use DeriveValue to perform operations on it.
 * TODO(Amadou): Rename to something more appropriate (like Row or Tuple?).
 */
class RowBatch {
 public:
  /**
   * Constructor
   * @param context compilation context
   * @param row_expr identifier of the row
   */
  explicit RowBatch(const CompilationContext &context, ast::IdentifierExpr *row_expr)
      : row_expr_(row_expr), context_(context) {}

  /// Prevent copy and move
  DISALLOW_COPY_AND_MOVE(RowBatch);

  /**
   * @return the identifier expr of the tuple
   */
  ast::IdentifierExpr *GetIdentifierExpr() { return row_expr_; }

  /**
   * Generates the TPL expression corresponding to ex
   * @param ex the expression to translation
   * @return the generated TPL expression.
   */
  ast::Expr *DeriveValue(const terrier::parser::AbstractExpression &ex);

 private:
  ast::IdentifierExpr *row_expr_;
  const CompilationContext &context_;
};
}  // namespace tpl::compiler
