#pragma once

#include <string>
#include <unordered_map>

#include "module.h"

// TODO (kjobanpu) put a future in module for async. make it synchronous for
//  just compiled tasks.
namespace terrier::execution::vm {
/**
 * A CompilationManager instance will handle asynchronous compilation tasks and
 * return back a handle to the user of the class.
 */
class CompilationManager {
 public:
  CompilationManager() = default;
  // Return machine code corresponding to handle provided.
  void *getMachineCode(std::string handle);
  void addModule(Module bytecode_module);

 private:
  class AsyncCompileTask;
  std::unordered_map <std::string, void*> handle_to_machine_code_;

};
}
