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

  explicit AsyncCompileTask(Module *module) : module_(module) {}

  // Execute
  tbb::task *execute() override {
    // This simply invokes Module::CompileToMachineCode() asynchronously.
    std::call_once(module_->compiled_flag_, [this]() {
      // Exit if the module has already been compiled. This might happen if
      // requested to execute in adaptive mode by concurrent threads.
      if (module_->jit_module_ != nullptr) {
        return;
      }

      // JIT the module.
      LLVMEngine::CompilerOptions options;
      module_->jit_module_ = LLVMEngine::Compile(*(module_->bytecode_module_), options);

      // JIT completed successfully. For each function in the module, pull out its
      // compiled implementation into the function cache, atomically replacing any
      // previous implementation.
      for (const auto &func_info : module_->bytecode_module_->GetFunctionsInfo()) {
        auto *jit_function = module_->jit_module_->GetFunctionPointer(func_info.GetName());
        NOISEPAGE_ASSERT(jit_function != nullptr, "Missing function in compiled module!");
        module_->functions_[func_info.GetId()].store(jit_function, std::memory_order_relaxed);
      }

      // handle_to_machine_code_[*module_] = module_->jit_module_;
    });
    // Done. There's no next task, so return null.
    return nullptr;
  }

 private:
  Module *module_;
  std::once_flag compiled_flag_;
};

void CompilationManager::AddModule(Module *module) {
  auto *compile_task = new (tbb::task::allocate_root()) AsyncCompileTask(module);
  tbb::task::enqueue(*compile_task);
}

void CompilationManager::TransferModule(std::unique_ptr<Module> &&module) { module_.push_back(std::move(module)); }

void CompilationManager::TransferContext(std::unique_ptr<util::Region> region) { region_.push_back(std::move(region)); }

}  // namespace noisepage::execution::vm
