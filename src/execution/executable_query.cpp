#include "execution/executable_query.h"

namespace terrier::execution {

ExecutableQuery::ExecutableQuery(const common::ManagedPointer<planner::AbstractPlanNode> physical_plan,
                                 const common::ManagedPointer<exec::ExecutionContext> exec_ctx) {
  // Compile and check for errors
  compiler::CodeGen codegen(exec_ctx.Get());
  compiler::Compiler compiler(&codegen, physical_plan.Get());
  auto root = compiler.Compile();
  if (codegen.Reporter()->HasErrors()) {
    EXECUTION_LOG_ERROR("Type-checking error! \n {}", codegen.Reporter()->SerializeErrors());
  }

  EXECUTION_LOG_INFO("Converted: \n {}", execution::ast::AstDump::Dump(root));

  // Convert to bytecode
  auto bytecode_module = vm::BytecodeGenerator::Compile(root, exec_ctx.Get(), "tmp-tpl");

  tpl_module_ = std::make_unique<vm::Module>(std::move(bytecode_module));
  region_ = codegen.ReleaseRegion();
  ast_ctx_ = codegen.ReleaseContext();
}

void ExecutableQuery::Run(const common::ManagedPointer<exec::ExecutionContext> exec_ctx, const vm::ExecutionMode mode) {
  // Run the main function
  std::function<int64_t(exec::ExecutionContext *)> main;
  if (!tpl_module_->GetFunction("main", mode, &main)) {
    EXECUTION_LOG_ERROR(
        "Missing 'main' entry function with signature "
        "(*ExecutionContext)->int32");
    return;
  }
  auto result = main(exec_ctx.Get());
  EXECUTION_LOG_INFO("main() returned: {}", result);
}

}  // namespace terrier::execution