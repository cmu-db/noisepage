#pragma once

#include "common/macros.h"
#include "execution/ast/ast.h"
#include "execution/compiler/compiler_defs.h"
#include "parser/expression/abstract_expression.h"

namespace tpl::compiler {
class CompilationContext;

class RowBatch {
 public:
  explicit RowBatch(const CompilationContext &context, ast::IdentifierExpr *row_expr)
      : row_expr_(row_expr), context_(context) {}
  DISALLOW_COPY_AND_MOVE(RowBatch);

  ast::IdentifierExpr *GetIdentifierExpr() { return row_expr_; }

  ast::Expr *DeriveValue(const terrier::parser::AbstractExpression &ex);

 private:
  ast::IdentifierExpr *row_expr_;
  const CompilationContext &context_;
};
}  // namespace tpl::compiler
