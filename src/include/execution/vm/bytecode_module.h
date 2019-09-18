#pragma once

#include <iosfwd>
#include <string>
#include <vector>

#include "execution/vm/bytecode_function_info.h"
#include "execution/vm/bytecode_iterator.h"
#include "execution/vm/vm.h"

namespace terrier::execution::vm {

/**
 * A module represents all code in a single TPL source file
 */
class BytecodeModule {
 public:
  /**
   * Construct a new bytecode module. After construction, all available bytecode
   * functions are available for execution.
   * @param name The name of the module
   * @param code The bytecode that makes up the module
   * @param functions The functions within the module
   */
  BytecodeModule(std::string name, std::vector<uint8_t> &&code, std::vector<FunctionInfo> &&functions);

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(BytecodeModule);

  /**
   * Look up a TPL function in this module by its ID
   * @return A pointer to the function's info if it exists; null otherwise
   */
  const FunctionInfo *GetFuncInfoById(const FunctionId func_id) const {
    TERRIER_ASSERT(func_id < NumFunctions(), "Invalid function");
    return &functions_[func_id];
  }

  /**
   * Look up a TPL function in this module by its name
   * @param name The name of the function to lookup
   * @return A pointer to the function's info if it exists; null otherwise
   */
  const FunctionInfo *GetFuncInfoByName(const std::string &name) const {
    for (const auto &func : functions_) {
      if (func.Name() == name) {
        return &func;
      }
    }
    return nullptr;
  }

  /**
   * Retrieve an iterator over the bytecode for the given function \a func
   * @return A pointer to the function's info if it exists; null otherwise
   */
  BytecodeIterator BytecodeForFunction(const FunctionInfo &func) const {
    // NOLINTNEXTLINE
    auto [start, end] = func.BytecodeRange();
    return BytecodeIterator(code_, start, end);
  }

  /**
   * Pretty print all the module's contents into the provided output stream
   * @param os The stream into which we dump the module's contents
   */
  void PrettyPrint(std::ostream *os);

  /**
   * Return the name of the module
   */
  const std::string &Name() const { return name_; }

  /**
   * Return a constant view of all functions
   */
  const std::vector<FunctionInfo> &Functions() const { return functions_; }

  /**
   * Return the number of bytecode instructions in this module
   */
  std::size_t InstructionCount() const { return code_.size(); }

  /**
   * Return the number of functions defined in this module
   */
  std::size_t NumFunctions() const { return functions_.size(); }

 private:
  friend class VM;

  const uint8_t *GetBytecodeForFunction(const FunctionInfo &func) const {
    // NOLINTNEXTLINE
    auto [start, _] = func.BytecodeRange();
    (void)_;
    return &code_[start];
  }

 private:
  const std::string name_;
  const std::vector<uint8_t> code_;
  const std::vector<FunctionInfo> functions_;
};

}  // namespace terrier::execution::vm
