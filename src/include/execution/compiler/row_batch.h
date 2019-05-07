#pragma once

#include "compiler_defs.h"

namespace tpl::ast {
class IdentifierExpr;
}

namespace tpl::compiler {
class RowBatch {
 public:
  explicit RowBatch(ast::IdentifierExpr *row_expr) : row_expr_(row_expr) {}
  DISALLOW_COPY_AND_MOVE(RowBatch);

  ast::IdentifierExpr *GetIdentifierExpr() { return row_expr_; }
 private:
  ast::IdentifierExpr *row_expr_;
};
}