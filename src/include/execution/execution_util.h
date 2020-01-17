#pragma once
#include <memory>
#include <utility>

#include "execution/util/cpu_info.h"
#include "execution/vm/llvm_engine.h"

namespace terrier::execution {

/**
 * Static helper methods for interacting with the LLVM execution engine.
 */
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
