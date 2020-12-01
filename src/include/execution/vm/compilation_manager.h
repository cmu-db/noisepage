#pragma once

#include <string>
#include <unordered_map>

#include "module.h"

#include "execution/ast/context.h"

namespace noisepage::execution::vm {
/**
 * A CompilationManager instance will handle asynchronous compilation tasks and
 * return back a handle to the user of the class.
 */
class CompilationManager {
 public:
  CompilationManager() = default;
  ~CompilationManager() = default;

  // Send a module to the compilation manager for compilation.
  void addModule(Module *module);

  void transferModule(std::unique_ptr<Module> &&module);

  void transferContext(std::unique_ptr<util::Region> region);

 private:
  class AsyncCompileTask;
  //std::unordered_map <Module, std::unique_ptr<LLVMEngine::CompiledModule>> handle_to_machine_code_;

  // TODO(Wuwen): implement a better data structure.
  std::vector<std::unique_ptr<Module>> module_;
  std::vector<std::unique_ptr<ast::Context>> context_;
  std::vector<std::unique_ptr<util::Region>> region_;

};
}
