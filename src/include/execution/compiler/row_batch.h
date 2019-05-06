#pragma once
#include "execution/ast/ast.h"
#include "execution/compiler/codegen.h"
#include "compiler_defs.h"

namespace tpl::compiler {
  class RowBatch {
   public:
    DISALLOW_COPY_AND_MOVE(RowBatch);

    explicit RowBatch(CodeGen &codeGen) : row_expr_(codeGen->NewIdentifierExpr(DUMMY_POS, codeGen.NewIdentifier())) {};
    explicit RowBatch(ast::IdentifierExpr *row_expr) : row_expr_(row_expr) {};

    ast::Identifier GetName() { return row_expr_->name(); }

    ast::IdentifierExpr *GetIdentifierExpr() { return row_expr_; }
   private:
    ast::IdentifierExpr *row_expr_;
  };
}