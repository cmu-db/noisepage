#pragma once

#include "execution/ast/ast_dump.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compiler.h"
#include "execution/util/cpu_info.h"
#include "execution/vm/bytecode_generator.h"
#include "execution/vm/llvm_engine.h"
#include "execution/vm/module.h"
#include "loggers/execution_logger.h"

namespace terrier::execution {

class ExecutionUtil {
 public:
  ExecutionUtil() = delete;

  static void CompileAndRun(planner::AbstractPlanNode *node, exec::ExecutionContext *exec_ctx) {
    // Create the query object, whose region must outlive all the processing.
    // Compile and check for errors
    compiler::CodeGen codegen(exec_ctx);
    compiler::Compiler compiler(&codegen, node);
    auto root = compiler.Compile();
    if (codegen.Reporter()->HasErrors()) {
      EXECUTION_LOG_ERROR("Type-checking error! \n {}", codegen.Reporter()->SerializeErrors());
    }

    EXECUTION_LOG_INFO("Converted: \n {}", execution::ast::AstDump::Dump(root));

    // Convert to bytecode
    auto bytecode_module = vm::BytecodeGenerator::Compile(root, exec_ctx, "tmp-tpl");
    auto module = std::make_unique<vm::Module>(std::move(bytecode_module));

    // Run the main function
    std::function<int64_t(exec::ExecutionContext *)> main;
    if (!module->GetFunction("main", vm::ExecutionMode::Interpret, &main)) {
      EXECUTION_LOG_ERROR(
          "Missing 'main' entry function with signature "
          "(*ExecutionContext)->int32");
      return;
    }
    EXECUTION_LOG_INFO("VM main() returned: {}", main(exec_ctx));
  }

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