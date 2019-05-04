#include "execution/compiler/function_builder.h"

namespace tpl::compiler {

FunctionBuilder::FunctionBuilder(CodeContext *code_ctx, std::string name, tpl::ast::Expr *ret_type)
: code_ctx_(code_ctx)
{

}
void FunctionBuilder::ReturnAndFinish(ast::Expr *ret_val) {

}

}