#pragma once

#include "execution/logging/logger.h"
#include "execution/util/common.h"
#include "execution/util/region_containers.h"
#include "execution/vm/bytecode_function_info.h"
#include "execution/vm/bytecodes.h"

namespace tpl::vm {

class BytecodeModule;

/// Our virtual machine
class VM {
 public:
  /// Invoke the function with ID \a func in the module \a module. \a args
  /// contains the output and input parameters stored contiguously.
  static void InvokeFunction(const BytecodeModule &module, FunctionId func_id,
                             const u8 args[]);

 private:
  // Private constructor to force users to use InvokeFunction
  explicit VM(const BytecodeModule &module);

  // This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(VM);

  // Forward declare the frame
  class Frame;

  // Interpret the given instruction stream using the given execution frame
  void Interpret(const u8 *ip, Frame *frame);

  // Execute a call instruction
  const u8 *ExecuteCall(const u8 *ip, Frame *caller);

  // Get the bytecode module handle
  const BytecodeModule &module() const { return module_; }

 private:
  // The module
  const BytecodeModule &module_;
};

}  // namespace tpl::vm
