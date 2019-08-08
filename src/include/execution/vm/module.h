#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <utility>

#include "llvm/Support/Memory.h"

#include "execution/ast/type.h"
#include "execution/vm/bytecode_module.h"
#include "execution/vm/llvm_engine.h"

namespace terrier::execution::vm::test {
class BytecodeTrampolineTest;
}  // namespace terrier::execution::vm::test

namespace terrier::execution::vm {

/**
 * An enumeration capturing different execution methods and optimization levels.
 */
enum class ExecutionMode : u8 {
  // Always execute in interpreted mode
  Interpret,
  // Execute in interpreted mode, but trigger a compilation asynchronously. As
  // compiled code becomes available, seamlessly swap it in and execute mixed
  // interpreter and compiled code.
  Adaptive,
  // Compile and generate all machine code before executing the function
  Compiled
};

/**
 * A Module instance is used to store all information associated with a single
 * TPL program. Module's are a top-level container for metadata about all TPL
 * functions, data structures, types, etc. They also contain the generated TBC
 * bytecode and their implementations, along with compiled machine-code versions
 * of TPL functions.
 *
 * Modules are thread-safe.
 */
class Module {
 public:
  /**
   * Create a TPL module using the given bytecode module as the only
   * implementation.
   * @param bytecode_module The bytecode module implementation.
   */
  explicit Module(std::unique_ptr<BytecodeModule> bytecode_module);

  /**
   * Construct a TPL module with the given bytecode and LLVM implementations.
   * @param bytecode_module The bytecode module implementation.
   * @param llvm_module The compiled code.
   */
  Module(std::unique_ptr<BytecodeModule> bytecode_module, std::unique_ptr<LLVMEngine::CompiledModule> llvm_module);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(Module);

  /**
   * Look up a TPL function in this module by its ID
   * @return A pointer to the function's info if it exists; null otherwise
   */
  const FunctionInfo *GetFuncInfoById(const FunctionId func_id) const {
    return bytecode_module_->GetFuncInfoById(func_id);
  }

  /**
   * Look up a TPL function in this module by its name
   * @param name The name of the function to lookup
   * @return A pointer to the function's info if it exists; null otherwise
   */
  const FunctionInfo *GetFuncInfoByName(const std::string &name) const {
    return bytecode_module_->GetFuncInfoByName(name);
  }

  /**
   * Retrieve and wrap a TPL function inside a C++ function object, thus making
   * the TPL function callable as a C++ function. Callers can request different
   * versions of the TPL code including an interpreted version and a compiled
   * version.
   * @tparam Ret Ret The C/C++ return type of the function
   * @tparam ArgTypes ArgTypes The C/C++ argument types to the function
   * @param name The name of the function the caller wants
   * @param exec_mode The execution mode (JIT or Interpret)
   * @param[out] func The function wrapper we use to wrap the TPL function
   * @return True if the function was found and the output parameter was set
   */
  template <typename Ret, typename... ArgTypes>
  bool GetFunction(const std::string &name, ExecutionMode exec_mode, std::function<Ret(ArgTypes...)> *func);

  /**
   * Return the raw function implementation for the function in this module with
   * ID @em func_id. The returned function will either be interpreted or
   * compiled, but the implementation is hidden from the caller.
   * @param func_id The ID of the function the caller wants.
   * @return The function address if it exists; null otherwise.
   */
  void *GetRawFunctionImpl(const FunctionId func_id) const {
    TPL_ASSERT(func_id < bytecode_module_->num_functions(), "Out-of-bounds function access");
    return functions_[func_id].load(std::memory_order_relaxed);
  }

  /**
   * Return the TPL bytecode module
   */
  const BytecodeModule *bytecode_module() const { return bytecode_module_.get(); }

 private:
  friend class VM;
  friend class AsyncCompileTask;
  friend class test::BytecodeTrampolineTest;

  // This class encapsulates the ability to asynchronously JIT compile a module.
  class AsyncCompileTask;

  // A trampoline is a stub function that serves as a landing point for all
  // functions executed in interpreted mode. The purpose of the trampoline is
  // to arrange and adjust call arguments from the C/C++ ABI to the TPL ABI.
  class Trampoline {
   public:
    // Create an empty/uninitialized trampoline
    Trampoline() = default;

    // Create a trampoline over the given memory block
    explicit Trampoline(llvm::sys::OwningMemoryBlock &&mem) noexcept : mem_(std::move(mem)) {}

    // Move assignment
    Trampoline &operator=(Trampoline &&other) noexcept {
      mem_ = std::move(other.mem_);
      return *this;
    }

    // Access the trampoline code
    void *code() const { return mem_.base(); }

   private:
    // Memory region where the trampoline's code is
    llvm::sys::OwningMemoryBlock mem_;
  };

  // Create a trampoline function for the function with id @em func_id
  void CreateFunctionTrampoline(FunctionId func_id);

  // Generate a trampoline for the function
  void CreateFunctionTrampoline(const FunctionInfo &func, Trampoline *trampoline);

  // Access the raw bytecode trampoline function
  void *GetBytecodeImpl(const FunctionId func_id) const { return bytecode_trampolines_[func_id].code(); }

  // Access the compiled implementation of the function with the given ID
  void *GetCompiledImpl(const FunctionId func_id) const {
    if (jit_module_ == nullptr) {
      return nullptr;
    }

    const auto *func_info = GetFuncInfoById(func_id);
    return jit_module_->GetFunctionPointer(func_info->name());
  }

  // Compile this module into machine code. This is a blocking call.
  void CompileToMachineCode();

  // Compile this module into machine code. This is a non-blocking call that
  // triggers a compilation in the background.
  void CompileToMachineCodeAsync();

 private:
  // The module containing all TBC (i.e., bytecode) for the TPL program.
  std::unique_ptr<BytecodeModule> bytecode_module_;
  // The module containing compiled machine code for the TPL program.
  std::unique_ptr<LLVMEngine::CompiledModule> jit_module_;
  // Function pointers for all functions defined in the TPL program. Pointers
  // may point into bytecode stub functions (i.e., interpreted implementations),
  // or into compiled machine-code implementations.
  std::unique_ptr<std::atomic<void *>[]> functions_;
  // Trampolines for all bytecode functions.
  std::unique_ptr<Trampoline[]> bytecode_trampolines_;
  // Compilation flag used to ensure compilation occurs only once, even under
  // concurrent invocations.
  std::once_flag compiled_flag_;
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

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

template <typename Ret, typename... ArgTypes>
inline bool Module::GetFunction(const std::string &name, const ExecutionMode exec_mode,
                                std::function<Ret(ArgTypes...)> *func) {
  // Lookup function
  const FunctionInfo *func_info = bytecode_module_->GetFuncInfoByName(name);

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
    case ExecutionMode::Adaptive: {
      CompileToMachineCodeAsync();
      FALLTHROUGH;
    }
    case ExecutionMode::Interpret: {
      *func = [this, func_info](ArgTypes... args) -> Ret {
        if constexpr (std::is_void_v<Ret>) {
          // Create a temporary on-stack buffer and copy all arguments
          u8 arg_buffer[(0ul + ... + sizeof(args))];
          detail::CopyAll(arg_buffer, args...);

          // Invoke and finish
          VM::InvokeFunction(this, func_info->id(), arg_buffer);
          return;
        }
        // The return value
        Ret rv{};

        // Create a temporary on-stack buffer and copy all arguments
        u8 arg_buffer[sizeof(Ret *) + (0ul + ... + sizeof(args))];
        detail::CopyAll(arg_buffer, &rv, args...);

        // Invoke and finish
        VM::InvokeFunction(this, func_info->id(), arg_buffer);
        return rv;
      };
      break;
    }
    case ExecutionMode::Compiled: {
      CompileToMachineCode();
      *func = [this, func_info](ArgTypes... args) -> Ret {
        void *raw_func = functions_[func_info->id()].load(std::memory_order_relaxed);
        auto *jit_f = reinterpret_cast<Ret (*)(ArgTypes...)>(raw_func);
        return jit_f(args...);
      };
      break;
    }
  }

  // Function is setup, return success
  return true;
}

}  // namespace terrier::execution::vm
