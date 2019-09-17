#pragma once

#include "execution/util/execution_common.h"
#include "execution/util/region_containers.h"
#include "execution/vm/bytecode_function_info.h"
#include "execution/vm/bytecodes.h"
#include "loggers/execution_logger.h"

namespace terrier::execution::vm {

class Module;

/**
 * Our virtual machine
 */
class VM {
 public:
  /**
   * Invoke the function with ID @em func_id in the module @em module. @em args
   * contains the output and input parameters stored contiguously.
   */
  static void InvokeFunction(const Module *module, FunctionId func_id, const uint8_t args[]);

 private:
  // Private constructor to force users to use InvokeFunction
  explicit VM(const Module *module);

  // This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(VM);

  // Forward declare the frame
  class Frame;

  // Interpret the given instruction stream using the given execution frame
  void Interpret(const uint8_t *ip, Frame *frame);

  // Execute a call instruction
  const uint8_t *ExecuteCall(const uint8_t *ip, Frame *caller);

 private:
  // The module
  const Module *module_;
};

}  // namespace terrier::execution::vm
