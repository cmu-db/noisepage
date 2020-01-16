#pragma once
#include <memory>
#include <utility>

#include "execution/ast/ast_dump.h"
#include "execution/ast/context.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compiler.h"
#include "execution/util/cpu_info.h"
#include "execution/util/region.h"
#include "execution/vm/bytecode_generator.h"
#include "execution/vm/llvm_engine.h"
#include "execution/vm/module.h"
#include "loggers/execution_logger.h"

namespace terrier::execution {

class ExecutableQuery {
 public:
  ExecutableQuery(const common::ManagedPointer<planner::AbstractPlanNode> physical_plan,
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

  void Run(const common::ManagedPointer<exec::ExecutionContext> exec_ctx, const vm::ExecutionMode mode) {
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

 private:
  // TPL bytecodes for this query
  std::unique_ptr<vm::Module> tpl_module_;

  // Memory region and AST context from the code generation stage that need to stay alive as long as the TPL module will
  // be executed. Direct access to these objects is likely unneeded from this class, we just want to tie the life cycles
  // together.
  std::unique_ptr<util::Region> region_;
  std::unique_ptr<ast::Context> ast_ctx_;
};

class ExecutionUtil {
 public:
  ExecutionUtil() = delete;

  /**
   * Initialize all TPL subsystems
   */
  static void InitTPL() {
    execution::CpuInfo::Instance();
    execution::vm::LLVMEngine::Initialize();
  }

  /**
   * Shutdown all TPL subsystems
   */
  static void ShutdownTPL() { terrier::execution::vm::LLVMEngine::Shutdown(); }
};

}  // namespace terrier::execution
