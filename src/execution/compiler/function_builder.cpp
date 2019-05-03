#include "execution/compiler/function_builder.h"

#include "execution/compiler/code_context.h"

namespace tpl::compiler {

FunctionBuilder::FunctionBuilder(tpl::compiler::CodeContext *ctx, std::string &name, tpl::ast::Type *ret_type,
                                 util::RegionVector<ast::FieldDecl*> &args, tpl::compiler::CodeGen *codeGen)
    : finished_(false), ctx_(ctx), codeGen_(codeGen), ret_type_(ret_type), args_(args), name_(name) {}

void FunctionBuilder::ReturnAndFinish(ast::Expr *ret) {
  if (finished_) {
    return;
  }
  CodeBlock &block = ctx_->GetCodeBlock();
  if (ret != nullptr) {
    block.Append(codeGen_->Return(ret));
  }
  auto compiled = codeGen_->GetFunction(name_, ret_type_, args_, block);
  finished_ = true;
  ctx_->FinishFunction(compiled);
}

}  // namespace tpl::compiler