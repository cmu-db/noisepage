#include "execution/compiler/function_builder.h"

#include <utility>
#include "execution/ast/ast.h"
#include "execution/ast/ast_dump.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compiler_defs.h"

namespace terrier::execution::compiler {

FunctionBuilder::FunctionBuilder(CodeGen *codegen, ast::Identifier fn_name,
                                 util::RegionVector<ast::FieldDecl *> &&fn_params, ast::Expr *fn_ret_type)
    : codegen_(codegen),
      fn_name_(fn_name),
      fn_params_(std::move(fn_params)),
      fn_ret_type_(fn_ret_type),
      fn_body_(codegen->EmptyBlock()),
      blocks_{fn_body_} {}

void FunctionBuilder::StartForStmt(ast::Stmt *init, ast::Expr *cond, ast::Stmt *next) {
  auto forblock = codegen_->EmptyBlock();
  Append(codegen_->Factory()->NewForStmt(DUMMY_POS, init, cond, next, forblock));
  blocks_.emplace_back(forblock);
}

void FunctionBuilder::StartIfStmt(ast::Expr *condition) {
  auto ifblock = codegen_->EmptyBlock();
  Append(codegen_->Factory()->NewIfStmt(DUMMY_POS, condition, ifblock, nullptr));
  blocks_.emplace_back(ifblock);
}

void FunctionBuilder::FinishBlockStmt() { blocks_.pop_back(); }

ast::FunctionDecl *FunctionBuilder::Finish() {
  for (const auto &stmt : final_stmts_) {
    fn_body_->AppendStmt(stmt);
  }
  auto fn_ty = codegen_->Factory()->NewFunctionType(DUMMY_POS, std::move(fn_params_), fn_ret_type_);
  auto fn_lit = codegen_->Factory()->NewFunctionLitExpr(fn_ty, fn_body_);
  blocks_.clear();
  return codegen_->Factory()->NewFunctionDecl(DUMMY_POS, fn_name_, fn_lit);
}

}  // namespace terrier::execution::compiler
