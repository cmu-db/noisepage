#pragma once

#include <algorithm>
#include <iosfwd>
#include <string>
#include <vector>

#include "execution/vm/bytecode_function_info.h"
#include "execution/vm/bytecode_iterator.h"
#include "execution/vm/vm.h"

namespace noisepage::execution::vm {

/**
 * A bytecode module is a container for all the TPL bytecode (TBC) for a TPL source file. Bytecode
 * modules directly contain a list of all the physical bytecode that make up the program, and a list
 * of functions that store information about the functions in the TPL program.
 */
class BytecodeModule {
 public:
  /**
   * Construct a new bytecode module. After construction, all available bytecode functions are
   * available for execution.
   * @param name The name of the module
   * @param code The code section containing all bytecode instructions.
   * @param data The data section containing static data.
   * @param functions The functions within the module
   * @param static_locals All statically allocated variables in the data section.
   */
  BytecodeModule(std::string name, std::vector<uint8_t> &&code, std::vector<uint8_t> &&data,
                 std::vector<FunctionInfo> &&functions, std::vector<LocalInfo> &&static_locals);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(BytecodeModule);

  /**
   * @return A pointer to the metadata of the function with the provided ID. No check is made to
   *         ensure the function is valid or belongs to this module.
   */
  const FunctionInfo *GetFuncInfoById(const FunctionId func_id) const {
    // Function IDs are dense, so the given ID must be in the range [0, # functions)
    NOISEPAGE_ASSERT(func_id < GetFunctionCount(), "Invalid function");
    return &functions_[func_id];
  }

  /**
   * @return A pointer to the metadata of the TPL function with the provided name in this module. If
   *         no such function exists, a NULL pointer is returned.
   */
  const FunctionInfo *LookupFuncInfoByName(const std::string &name) const {
    for (const FunctionInfo &info : functions_) {
      if (info.GetName() == name) {
        return &info;
      }
    }
    return nullptr;
  }

  /**
   * @return A pointer to the metadata of the static local variable at the provided offset. If no
   *         such static local exists, a NULL pointer is returned.
   */
  const LocalInfo *LookupStaticInfoByOffset(const uint32_t offset) const {
    for (const LocalInfo &info : static_locals_) {
      if (info.GetOffset() == offset) {
        return &info;
      }
    }
    return nullptr;
  }

  /**
   * @return An iterator over the bytecode for the function @em func.
   */
  BytecodeIterator GetBytecodeForFunction(const FunctionInfo &func) const {
    const auto [start, end] = func.GetBytecodeRange();
    return BytecodeIterator(code_, start, end);
  }

  /**
   * @return The number of bytecode instructions in this module.
   */
  std::size_t GetInstructionCount() const;

  /**
   * @return The name of the module.
   */
  const std::string &GetName() const { return name_; }

  /**
   * @return A const-view of the metadata for all functions in this module.
   */
  const std::vector<FunctionInfo> &GetFunctionsInfo() const { return functions_; }

  /**
   * @return A const-view of the metadata for all static-locals in this module.
   */
  const std::vector<LocalInfo> &GetStaticLocalsInfo() const { return static_locals_; }

  /**
   * @return The number of functions defined in this module.
   */
  std::size_t GetFunctionCount() const { return functions_.size(); }

  /**
   * @return The number of static locals.
   */
  uint32_t GetStaticLocalsCount() const { return static_locals_.size(); }

  /**
   * Pretty print all the module's contents into the provided output stream.
   * @param os The stream into which we dump the module's contents.
   */
  void Dump(std::ostream &os) const;

 private:
  friend class VM;
  friend class LLVMEngine;

  // Access the raw bytecode for the given function. Unlike the public
  // GetBytecodeForFunction(), this function doesn't return an iterator. It
  // returns a pointer the raw underlying bytecode. It's used by the VM to
  // execute functions.
  const uint8_t *AccessBytecodeForFunctionRaw(const FunctionInfo &func) const {
    NOISEPAGE_ASSERT(GetFuncInfoById(func.GetId()) == &func, "Function not in module!");
    auto [start, _] = func.GetBytecodeRange();
    (void)_;
    return &code_[start];
  }

  // Access a const-view of some static-local's data by its offset.
  const uint8_t *AccessStaticLocalDataRaw(const uint32_t offset) const {
#ifndef NDEBUG
    // In DEBUG mode, make sure the offset we're about to access corresponds to a valid static value
    NOISEPAGE_ASSERT(offset < data_.size(), "Invalid local offset");
    NOISEPAGE_ASSERT(
        std::find_if(static_locals_.begin(), static_locals_.end(),
                     [&](const LocalInfo &info) { return info.GetOffset() == offset; }) != static_locals_.end(),
        "No local at given offset");
#endif
    return &data_[offset];
  }

  // Access a const-view of a static-local's data.
  const uint8_t *AccessStaticLocalDataRaw(const LocalVar local) const {
    return AccessStaticLocalDataRaw(local.GetOffset());
  }

 private:
  // The name of the module.
  const std::string name_;
  // The raw bytecode for ALL functions store contiguously.
  const std::vector<uint8_t> code_;
  // The raw static data for ALL static data stored contiguously.
  const std::vector<uint8_t> data_;
  // Metadata for all functions.
  const std::vector<FunctionInfo> functions_;
  // Metadata for all static data.
  const std::vector<LocalInfo> static_locals_;
};

}  // namespace noisepage::execution::vm
