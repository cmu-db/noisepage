#pragma once

#include <functional>
#include <iosfwd>
#include <string>
#include <utility>
#include <vector>

#include "llvm/Support/Memory.h"

#include "execution/ast/type.h"
#include "execution/util/memory.h"
#include "execution/vm/bytecode_function_info.h"
#include "execution/vm/bytecode_iterator.h"
#include "execution/vm/llvm_engine.h"
#include "execution/vm/vm.h"

#include "loggers/execution_logger.h"

namespace tpl::vm {

/**
 * An enumeration capturing different execution methods and optimization levels
 */
enum class ExecutionMode : u8 { Interpret, Jit };

/**
 * A module represents all code in a single TPL source file
 */
class BytecodeModule {
 public:
  /**
   * Construct
   * @param name name of the bytecode module
   * @param code bytecodes
   * @param functions list of functions
   */
  BytecodeModule(std::string name, std::vector<u8> &&code, std::vector<FunctionInfo> &&functions);

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(BytecodeModule);

  /**
   * Look up a TPL function in this module by its ID
   * @param func_id function id
   * @return A pointer to the function's info if it exists; null otherwise
   */
  const FunctionInfo *GetFuncInfoById(const FunctionId func_id) const {
    TPL_ASSERT(func_id < num_functions(), "Invalid function");
    return &functions_[func_id];
  }

  /**
   * Look up a TPL function in this module by its name
   * @param name function name
   * @return A pointer to the function's info if it exists; null otherwise
   */
  const FunctionInfo *GetFuncInfoByName(const std::string &name) const {
    for (const auto &func : functions_) {
      if (func.name() == name) {
        return &func;
      }
    }
    return nullptr;
  }

  /**
   * Retrieve an iterator over the bytecode for the given function \a func
   * @param func function to retrieve
   * @return A pointer to the function's info if it exists; null otherwise
   */
  BytecodeIterator BytecodeForFunction(const FunctionInfo &func) const {
    auto range = func.bytecode_range();
    auto start = range.first;
    auto end = range.second;
    return BytecodeIterator(code_, start, end);
  }

  /**
   * Get the trampoline for the bytecode function with id func_id
   * @param func_id function is
   * @return An opaque function pointer to the bytecode function
   */
  void *GetFuncTrampoline(const FunctionId func_id) const {
    TPL_ASSERT(func_id < num_functions(), "Invalid function");
    return trampolines_[func_id].GetCode();
  }

  /**
   * Retrieve and wrap a TPL function inside a C++ function object, thus making
   * the TPL function callable as a C++ function. Callers can request different
   * versions of the TPL code including an interpreted version and a compiled
   * version.
   * @tparam Ret Ret The C/C++ return type of the function
   * @tparam ArgTypes ArgTypes The C/C++ argument types to the function
   * @param name The name of the function the caller wants
   * @param exec_mode The interpretation mode the caller desires
   * @param func The function wrapper we use to wrap the TPL function
   * @return True if the function was found and the output parameter was set
   */
  template <typename Ret, typename... ArgTypes>
  bool GetFunction(const std::string &name, ExecutionMode exec_mode, std::function<Ret(ArgTypes...)> *func);

  /**
   * Pretty print all the module's contents into the provided output stream
   * @param os The stream into which we dump the module's contents
   */
  void PrettyPrint(std::ostream &os);

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  /**
   * @return the name of the module
   */
  const std::string &name() const { return name_; }

  /**
   * @return a constant view of all functions
   */
  const std::vector<FunctionInfo> &functions() const { return functions_; }

  /**
   * @return the number of bytecode instructions in this module
   */
  std::size_t instruction_count() const { return code_.size(); }

  /**
   * @return the number of functions defined in this module
   */
  std::size_t num_functions() const { return functions_.size(); }

 private:
  friend class VM;
  friend class LLVMEngine;

  const u8 *GetBytecodeForFunction(const FunctionInfo &func) const {
    auto range = func.bytecode_range();
    auto start = range.first;
    return &code_[start];
  }

  /// Create a trampoline function for the function with id \a func_id
  class Trampoline;
  void CreateFunctionTrampoline(FunctionId func_id);
  void CreateFunctionTrampoline(const FunctionInfo &func, Trampoline *trampoline);

 private:
  /// A trampoline is a stub function that all calls into TPL code go through
  /// to set up call arguments.
  class Trampoline {
   public:
    /// Create an empty/uninitialized trampoline
    Trampoline() noexcept {};  // NOLINT

    /// Create a trampoline over the given memory block
    explicit Trampoline(llvm::sys::OwningMemoryBlock &&mem) noexcept : mem_(std::move(mem)) {}

    /// Move assignment
    Trampoline &operator=(Trampoline &&other) noexcept {
      mem_ = std::move(other.mem_);
      return *this;
    }

    /// Access the trampoline code
    void *GetCode() const { return mem_.base(); }

   private:
    // Memory region where the trampoline's code is
    llvm::sys::OwningMemoryBlock mem_;
  };

 private:
  const std::string name_;
  const std::vector<u8> code_;
  const std::vector<FunctionInfo> functions_;
  std::vector<Trampoline> trampolines_;
};

//----------------------------------------------------------
// Implementation below
//----------------------------------------------------------

namespace detail {

// These functions value-copy a variable number of pass-by-value arguments into
// a given buffer. It's assumed the buffer is large enough to hold all arguments

inline void CopyAll(UNUSED u8 *buffer) {}

template <typename HeadT, typename... RestT>
inline void CopyAll(u8 *buffer, const HeadT &head, const RestT &... rest) {
  std::memcpy(buffer, reinterpret_cast<const u8 *>(&head), sizeof(head));
  CopyAll(buffer + sizeof(head), rest...);
}

}  // namespace detail

/**
 * Get a function by name
 * @tparam RetT return type of the function
 * @tparam ArgTypes types of the argurments to the functions
 * @param name name of the function
 * @param exec_mode execution mode (JIT or interpret)
 * @param func output variable where the function address is store
 * @return whether the function was found and the types match.
 */
template <typename RetT, typename... ArgTypes>
inline bool BytecodeModule::GetFunction(const std::string &name, ExecutionMode exec_mode,
                                        std::function<RetT(ArgTypes...)> *func) {
  const FunctionInfo *func_info = GetFuncInfoByName(name);

  // Check valid function
  if (func_info == nullptr) {
    return false;
  }

  // Verify argument counts
  constexpr const u32 num_params = sizeof...(ArgTypes);
  if (num_params != func_info->func_type()->num_params()) {
    return false;
  }

  switch (exec_mode) {
    case ExecutionMode::Interpret: {
      *func = [this, func_info](ArgTypes... args) -> RetT {
        if constexpr (std::is_void_v<RetT>) {
          // Create a temporary on-stack buffer and copy all arguments
          u8 arg_buffer[(0ul + ... + sizeof(args))];
          detail::CopyAll(arg_buffer, args...);

          // Invoke and finish
          VM::InvokeFunction(*this, func_info->id(), arg_buffer);
          return;
        }
        // The return value
        RetT rv{};
        // Create a temporary on-stack buffer and copy all arguments
        u8 arg_buffer[sizeof(RetT *) + (0ul + ... + sizeof(args))];
        detail::CopyAll(arg_buffer, &rv, args...);
        // Invoke and finish
        VM::InvokeFunction(*this, func_info->id(), arg_buffer);
        return rv;
      };
      break;
    }
    case ExecutionMode::Jit: {
      *func = [this, func_info](ArgTypes... args) -> RetT {
        // TODO(pmenon): Check if already compiled

        // JIT the module
        auto compiled = LLVMEngine::Compile(*this);

        void *raw_fn = compiled->GetFunctionPointer(func_info->name());
        TPL_ASSERT(raw_fn != nullptr, "No function");

        if constexpr (std::is_void_v<RetT>) {
          auto *jit_f = reinterpret_cast<void (*)(ArgTypes...)>(raw_fn);
          jit_f(args...);
          return;
        }
        auto *jit_f = reinterpret_cast<void (*)(RetT *, ArgTypes...)>(raw_fn);
        RetT rv{};
        jit_f(&rv, args...);
        return rv;
      };
      break;
    }
  }

  // Function is setup, return success
  return true;
}

}  // namespace tpl::vm
