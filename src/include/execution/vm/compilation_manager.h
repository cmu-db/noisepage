#pragma once

#include <string>
#include <unordered_map>

#include "module.h"

namespace noisepage::execution::vm {
/**
 * A CompilationManager instance will handle asynchronous compilation tasks and
 * return back a handle to the user of the class.
 */
class CompilationManager {
 public:
  CompilationManager() = default;


  // TODO: come up with a handle type that makes sense for the map
  // Return machine code corresponding to handle provided.
  //std::unique_ptr<LLVMEngine::CompiledModule> getMachineCode(Module *module);

  // Send a module to the compilation manager for compilation.
  void addModule(Module *module);

 private:
  class AsyncCompileTask;
  //std::unordered_map <Module, std::unique_ptr<LLVMEngine::CompiledModule>> handle_to_machine_code_;
};
}
