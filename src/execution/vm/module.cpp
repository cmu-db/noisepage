#include "execution/vm/module.h"

#include <tbb/task.h>  // NOLINT

#include <memory>
#include <mutex>  // NOLINT
#include <string>
#include <utility>

#include "loggers/execution_logger.h"

#define XBYAK_NO_OP_NAMES
#include "xbyak/xbyak.h"

namespace noisepage::execution::vm {

// ---------------------------------------------------------
// Async Compile Task
// ---------------------------------------------------------

// This class encapsulates the ability to asynchronously JIT compile a module.
class Module::AsyncCompileTask : public tbb::task {
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

// ---------------------------------------------------------
// Module
// ---------------------------------------------------------

Module::Module(std::unique_ptr<BytecodeModule> bytecode_module) : Module(std::move(bytecode_module), nullptr) {}

Module::Module(std::unique_ptr<BytecodeModule> bytecode_module, std::unique_ptr<LLVMEngine::CompiledModule> llvm_module)
    : bytecode_module_(std::move(bytecode_module)),
      jit_module_(std::move(llvm_module)),
      functions_(std::make_unique<std::atomic<void *>[]>(bytecode_module_->GetFunctionCount())),
      bytecode_trampolines_(std::make_unique<Trampoline[]>(bytecode_module_->GetFunctionCount())) {
  // Create the trampolines for all bytecode functions
  for (const auto &func : bytecode_module_->GetFunctionsInfo()) {
    CreateFunctionTrampoline(func.GetId());
  }

  // If a compiled module wasn't provided, all internal function stubs point to
  // the bytecode implementations.
  if (jit_module_ == nullptr) {
    const auto num_functions = bytecode_module_->GetFunctionCount();
    for (uint32_t idx = 0; idx < num_functions; idx++) {
      functions_[idx] = bytecode_trampolines_[idx].GetCode();
    }
  } else {
    const auto num_functions = bytecode_module_->GetFunctionCount();
    for (uint32_t idx = 0; idx < num_functions; idx++) {
      auto func_info = bytecode_module_->GetFuncInfoById(idx);
      functions_[idx] = jit_module_->GetFunctionPointer(func_info->GetName());
    }
  }
}

namespace {

// TODO(pmenon): Implement generator for non x86_64 machines
// TODO(pmenon): Implement generator for Windows
// TODO(pmenon): Implement non-integer input and output arguments
// TODO(pmenon): Handle more than 6 input arguments
// TODO(pmenon): **LOTS** of shit to make this fully ABI compliant ....
class TrampolineGenerator : public Xbyak::CodeGenerator {
 public:
  TrampolineGenerator(const Module &module, const FunctionInfo &func_info, void *mem)
      : Xbyak::CodeGenerator(Xbyak::DEFAULT_MAX_CODE_SIZE, mem), module_(module), func_(func_info) {}

  /// Generate trampoline code for the given function in the given module
  void Generate() {
    // Compute the stack space needed for all arguments
    const uint32_t required_stack_space = ComputeRequiredStackSpace();

    // Function prologue
    Prologue();

    // Allocate space for arguments
    AllocStack(required_stack_space);

    // Push call args onto stack
    PushCallerArgsOntoStack();

    // Invoke the VM entry function
    InvokeVMFunction();

    // Restore stack
    FreeStack(required_stack_space);

    // Function epilogue
    Epilogue();

    // Return from the trampoline, placing return values where appropriate
    Return();
  }

 private:
  uint32_t ComputeRequiredStackSpace() const {
    // FunctionInfo tells us the amount of space we need for all input and
    // output arguments, so use that.
    uint32_t required_stack_space = func_.GetParamsSize();

    // If the function has a return type, we need to allocate a temporary
    // return value on the stack for that as well. However, if the return type
    // is larger than 8 bytes (i.e., larger than a general-purpose register),
    // a pointer to the return value is provided to the trampoline as the first
    // argument
    const ast::Type *return_type = func_.GetFuncType()->GetReturnType();
    if (!return_type->IsNilType()) {
      required_stack_space += common::MathUtil::AlignTo(return_type->GetSize(), sizeof(intptr_t));
    }

    // Always align to cacheline boundary
    return common::MathUtil::AlignTo(required_stack_space, common::Constants::CACHELINE_SIZE);
  }

  void Prologue() { push(rbx); }

  void Epilogue() { pop(rbx); }

  void AllocStack(uint32_t size) { sub(rsp, size); }

  void FreeStack(uint32_t size) { add(rsp, size); }

  // This function pushes all caller arguments onto the stack. We assume that
  // there is enough stack through a previous call to AdjustStack(). There are
  // a few different cases to handle:
  //
  // If the function returns a value whose size is less than 8-bytes, we want
  // the stack to look as follows:
  //
  //            |                   |
  //  Old SP ━> +-------------------+ (Higher address)
  //            |       Arg N       |
  //            +-------------------+
  //            |        ...        |
  //            +-------------------+
  //            |       Arg 1       |
  //            +-------------------+
  //        ┏━━ |  Pointer to 'rv'  |
  //        ┃   +-------------------+
  //        ┗━> | Return Value 'rv' |
  //  New SP ━> +-------------------+ (Lower address)
  //            |                   |
  //
  // If the function is a void function neither a return value or a pointer to
  // it are allocated on the stack. Only legitimate function arguments will be
  // placed on the stack.
  //
  void PushCallerArgsOntoStack() {
    const Xbyak::Reg arg_regs[][6] = {{edi, esi, edx, ecx, r8d, r9d}, {rdi, rsi, rdx, rcx, r8, r9}};

    const ast::FunctionType *func_type = func_.GetFuncType();
    NOISEPAGE_ASSERT(func_type->GetNumParams() < sizeof(arg_regs), "Too many function arguments");

    uint32_t displacement = 0;
    uint32_t local_idx = 0;

    // The first argument to the TBC function is a pointer to the return value.
    // If the function returns a non-void value, insert the pointer now.
    if (const ast::Type *return_type = func_type->GetReturnType(); !return_type->IsNilType()) {
      displacement = common::MathUtil::AlignTo(return_type->GetSize(), sizeof(intptr_t));
      mov(ptr[rsp + displacement], rsp);
      local_idx++;
    }

    // Now push all input arguments.
    for (uint32_t idx = 0; idx < func_type->GetNumParams(); idx++, local_idx++) {
      const auto &local_info = func_.GetLocals()[local_idx];
      auto use_64bit_reg = static_cast<uint32_t>(local_info.GetSize() > sizeof(uint32_t));
      mov(ptr[rsp + displacement + local_info.GetOffset()], arg_regs[use_64bit_reg][idx]);
    }
  }

  void InvokeVMFunction() {
    const ast::FunctionType *func_type = func_.GetFuncType();
    const ast::Type *ret_type = func_type->GetReturnType();
    uint32_t ret_type_size = 0;
    if (!ret_type->IsNilType()) {
      ret_type_size = common::MathUtil::AlignTo(ret_type->GetSize(), sizeof(intptr_t));
    }

    // Set up the arguments to VM::InvokeFunction(module, function ID, args)
    mov(rdi, reinterpret_cast<std::size_t>(&module_));
    mov(rsi, func_.GetId());
    lea(rdx, ptr[rsp + ret_type_size]);

    // Call VM::InvokeFunction()
    mov(rax, reinterpret_cast<std::size_t>(&VM::InvokeFunction));
    call(rax);

    if (!ret_type->IsNilType()) {
      if (ret_type->GetSize() < 8) {
        mov(eax, ptr[rsp]);
      } else {
        mov(rax, ptr[rsp]);
      }
    }
  }

  void Return() { ret(); }

 private:
  const Module &module_;
  const FunctionInfo &func_;
};

}  // namespace

void Module::CreateFunctionTrampoline(const FunctionInfo &func, Trampoline *trampoline) {
  // TODO(pmenon): Is 4KB too large? Should it be dynamic?
  static constexpr std::size_t default_code_size = 4 * common::Constants::KB;

  // Allocate memory for the trampoline.
  std::error_code error;
  const int32_t rw_flags = llvm::sys::Memory::MF_READ | llvm::sys::Memory::MF_WRITE;
  llvm::sys::MemoryBlock memory = llvm::sys::Memory::allocateMappedMemory(default_code_size, nullptr, rw_flags, error);
  if (error) {
    EXECUTION_LOG_ERROR("There was an error allocating executable memory {}", error.message());
    return;
  }

  // Generate code!
  TrampolineGenerator generator(*this, func, memory.base());
  generator.Generate();

  // Now that the code's been generated and finalized, let's remove write
  // protections and just make is read+exec.
  const int32_t rx_flags = llvm::sys::Memory::MF_READ | llvm::sys::Memory::MF_EXEC;
  llvm::sys::Memory::protectMappedMemory(memory, rx_flags);

  // Done.
  *trampoline = Trampoline(llvm::sys::OwningMemoryBlock(memory));
}

void Module::CreateFunctionTrampoline(FunctionId func_id) {
  // If a trampoline has already been setup, don't bother.
  if (bytecode_trampolines_[func_id].GetCode() != nullptr) {
    EXECUTION_LOG_DEBUG("Function {} has a trampoline; will not recreate", func_id);
    return;
  }

  // Create the trampoline for the function.
  Trampoline trampoline;
  const auto *func_info = GetFuncInfoById(func_id);
  CreateFunctionTrampoline(*func_info, &trampoline);

  // Mark available.
  bytecode_trampolines_[func_id] = std::move(trampoline);
}

void Module::CompileToMachineCode() {
  std::call_once(compiled_flag_, [this]() {
    // Exit if the module has already been compiled. This might happen if
    // requested to execute in adaptive mode by concurrent threads.
    if (jit_module_ != nullptr) {
      return;
    }

    // JIT the module.
    LLVMEngine::CompilerOptions options;
    jit_module_ = LLVMEngine::Compile(*bytecode_module_, options);

    // JIT completed successfully. For each function in the module, pull out its
    // compiled implementation into the function cache, atomically replacing any
    // previous implementation.
    for (const auto &func_info : bytecode_module_->GetFunctionsInfo()) {
      auto *jit_function = jit_module_->GetFunctionPointer(func_info.GetName());
      NOISEPAGE_ASSERT(jit_function != nullptr, "Missing function in compiled module!");
      functions_[func_info.GetId()].store(jit_function, std::memory_order_relaxed);
    }
  });
}

void Module::CompileToMachineCodeAsync() {
  auto *compile_task = new (tbb::task::allocate_root()) AsyncCompileTask(this);
  tbb::task::enqueue(*compile_task);
}

}  // namespace noisepage::execution::vm
