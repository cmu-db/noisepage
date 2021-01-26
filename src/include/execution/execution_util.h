#pragma once
#include <memory>
#include <string_view>
#include <utility>

#include "execution/util/cpu_info.h"
#include "execution/vm/llvm_engine.h"

namespace noisepage::execution {

/**
 * Static helper methods for interacting with the LLVM execution engine.
 */
class ExecutionUtil {
 public:
  ExecutionUtil() = delete;

  /**
   * Initialize all TPL subsystems
   */
  static void InitTPL(std::string_view bytecode_handlers_path) {
    execution::CpuInfo::Instance();
    execution::vm::LLVMEngine::Initialize(bytecode_handlers_path);
  }

  /**
   * Shutdown all TPL subsystems
   */
  static void ShutdownTPL() { noisepage::execution::vm::LLVMEngine::Shutdown(); }
};

}  // namespace noisepage::execution
