#pragma once

#include <string>
#include "execution/ast/ast.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compiler_defs.h"
#include "execution/util/region_containers.h"

namespace tpl::compiler {

class CodeGen;

class FunctionBuilder {
 public:
  FunctionBuilder(CodeGen *codegen, ast::Identifier fn_name, util::RegionVector<ast::FieldDecl *> fn_params,
                  ast::Expr *fn_ret_type);

  DISALLOW_COPY_AND_MOVE(FunctionBuilder);

  void Append(ast::Stmt *stmt) { insertion_point_->AppendStmt(stmt); }

  void SetInsertionPoint(ast::BlockStmt *insertion_point) { insertion_point_ = insertion_point; }

  void StartForInStmt(ast::Expr *target, ast::Expr *table, ast::Attributes *attributes);

  void StartIfStmt(ast::Expr *condition);

  CodeGen *GetCodeGen() { return codegen_; }

  ast::FunctionDecl *Finish();

 private:
  CodeGen *codegen_;
  FunctionBuilder *prev_fn_;

  ast::Identifier fn_name_;
  util::RegionVector<ast::FieldDecl *> fn_params_;
  ast::Expr *fn_ret_type_;

  ast::BlockStmt *fn_body_;
  ast::BlockStmt *insertion_point_;
};

}  // namespace tpl::compiler
