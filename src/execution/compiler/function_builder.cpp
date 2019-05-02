#include "execution/compiler/function_builder.h"

#include "execution/compiler/code_context.h"

namespace tpl::compiler {

FunctionBuilder::FunctionBuilder(tpl::compiler::CodeContext *ctx,
                                 std::string name,
                                 tpl::ast::Type *ret_type,
                                 tpl::util::RegionVector<tpl::ast::FieldDecl> args)
                                 : finished_(false), ctx_(ctx), prev_fn_(ctx->GetCurrentFunction())
                                 {}


}