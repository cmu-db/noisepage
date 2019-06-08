#include "execution/compiler/function_builder.h"

#include <utility>
#include "execution/ast/ast.h"
#include "execution/ast/ast_dump.h"
#include "execution/compiler/code_context.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compiler_defs.h"

namespace tpl::compiler {

FunctionBuilder::FunctionBuilder(CodeGen *codegen, ast::Identifier fn_name,
                                 util::RegionVector<ast::FieldDecl *> fn_params, ast::Expr *fn_ret_type)
    : codegen_(codegen),
      prev_fn_(codegen_->GetCodeContext()->GetCurrentFunction()),
      fn_name_(fn_name),
      fn_params_(std::move(fn_params)),
      fn_ret_type_(fn_ret_type),
      fn_body_(codegen->EmptyBlock()),
      insertion_point_(fn_body_) {
  codegen->GetCodeContext()->SetCurrentFunction(this);
}

void FunctionBuilder::StartForInStmt(ast::Expr *target, ast::Expr *table, ast::Attributes *attributes) {
  auto forblock = codegen_->EmptyBlock();
  Append((*codegen_)->NewForInStmt(DUMMY_POS, target, table, attributes, forblock));
  SetInsertionPoint(forblock);
}

void FunctionBuilder::StartIfStmt(ast::Expr *condition) {
  auto ifblock = codegen_->EmptyBlock();
  Append((*codegen_)->NewIfStmt(DUMMY_POS, condition, ifblock, nullptr));
  SetInsertionPoint(ifblock);
}

ast::FunctionDecl *FunctionBuilder::Finish() {
  auto fn_ty = (*codegen_)->NewFunctionType(DUMMY_POS, std::move(fn_params_), fn_ret_type_);
  auto fn_lit = (*codegen_)->NewFunctionLitExpr(fn_ty, fn_body_);
  codegen_->GetCodeContext()->SetCurrentFunction(prev_fn_);
  return (*codegen_)->NewFunctionDecl(DUMMY_POS, fn_name_, fn_lit);
}

}  // namespace tpl::compiler
