#include "execution/vm/compilation_manager.h"

#include <tbb/task.h>

namespace terrier::execution::vm {
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
    module_->CompileToMachineCode();
    // Done. There's no next task, so return null.
    return nullptr;
  }

 private:
  Module *module_;
};

void *terrier::execution::vm::CompilationManager::getMachineCode(std::string handle) { return nullptr; }
void terrier::execution::vm::CompilationManager::addModule(terrier::execution::vm::Module module) {}


}
