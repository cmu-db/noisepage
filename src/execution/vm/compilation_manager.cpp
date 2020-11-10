#include "execution/vm/compilation_manager.h"

#include <tbb/task.h>

namespace noisepage::execution::vm {
// ---------------------------------------------------------
// Async Compile Task
// ---------------------------------------------------------

// This class encapsulates the ability to asynchronously JIT compile a module.
class CompilationManager::AsyncCompileTask : public tbb::task {
 public:
  // Construct an asynchronous compilation task to compile the the module
  explicit AsyncCompileTask(std::shared_ptr<BytecodeModule> bytecode_module)
      : bytecode_module_(bytecode_module),
        functions_(std::atomic<void *>[](bytecode_module_->GetFunctionCount())){}

  // Execute
  tbb::task *execute() override {
      // JIT the module.
    LLVMEngine::CompilerOptions options;
    auto jit_module = LLVMEngine::Compile(*bytecode_module_, options);

    // JIT completed successfully. For each function in the module, pull out its
    // compiled implementation into the function cache, atomically replacing any
    // previous implementation.
    for (const auto &func_info : bytecode_module_->GetFunctionsInfo()) {
      auto *jit_function = jit_module->GetFunctionPointer(func_info.GetName());
      NOISEPAGE_ASSERT(jit_function != nullptr, "Missing function in compiled module!");
      functions_[func_info.GetId()].store(jit_function, std::memory_order_relaxed);
    }

    // Done. There's no next task, so return null.
    return nullptr;
  };

 private:
  // TODO: is the poking mechanism going to be a future / promise?
  std::shared_ptr<BytecodeModule> bytecode_module_;
  std::atomic<void *>[] *functions_;
};

/*
std::unique_ptr<LLVMEngine::CompiledModule> CompilationManager::getMachineCode(Module *module) {
  if (handle_to_machine_code_.find(*module) != handle_to_machine_code_.end()) {
    return handle_to_machine_code_[*module];
  }
}
*/

void CompilationManager::addModule(std::shared_ptr<BytecodeModule> bytecode_module) {
  auto *compile_task = new (tbb::task::allocate_root()) AsyncCompileTask(bytecode_module);
  tbb::task::enqueue(*compile_task);
}

}  // namespace noisepage::execution::vm
