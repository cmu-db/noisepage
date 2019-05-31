#include "execution/vm/module.h"

#include <memory>
#include <mutex>  // NOLINT
#include <string>
#include <utility>

#include <tbb/task.h>  // NOLINT

#define XBYAK_NO_OP_NAMES
#include "xbyak/xbyak/xbyak.h"

namespace tpl::vm {

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
      functions_(std::make_unique<std::atomic<void *>[]>(bytecode_module_->num_functions())),
      bytecode_trampolines_(std::make_unique<Trampoline[]>(bytecode_module_->num_functions())) {
  // Create the trampolines for all bytecode functions
  for (const auto &func : bytecode_module_->functions()) {
    CreateFunctionTrampoline(func.id());
  }

  // If a compiled module wasn't provided, all internal function stubs point to
  // the bytecode implementations.
  if (jit_module_ == nullptr) {
    const auto num_functions = bytecode_module_->num_functions();
    for (u32 idx = 0; idx < num_functions; idx++) {
      functions_[idx] = bytecode_trampolines_[idx].code();
    }
  } else {
    const auto num_functions = bytecode_module_->num_functions();
    for (u32 idx = 0; idx < num_functions; idx++) {
      auto func_info = bytecode_module_->GetFuncInfoById(static_cast<u16>(idx));
      functions_[idx] = jit_module_->GetFunctionPointer(func_info->name());
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

  /**
   * Generate trampoline code for the given function in the given module
   */
  void Generate() {
    // Compute the stack space needed for all arguments
    const u32 required_stack_space = ComputeRequiredStackSpace();

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
  u32 ComputeRequiredStackSpace() const {
    // FunctionInfo tells us the amount of space we need for all input and
    // output arguments, so use that.
    auto required_stack_space = static_cast<u32>(func_.params_size());

    // If the function has a return type, we need to allocate a temporary
    // return value on the stack for that as well. However, if the return type
    // is larger than 8 bytes (i.e., larger than a general-purpose register),
    // a pointer to the return value is provided to the trampoline as the first
    // argument
    const ast::Type *return_type = func_.func_type()->return_type();
    if (!return_type->IsNilType()) {
      required_stack_space += static_cast<u32>(util::MathUtil::AlignTo(return_type->size(), sizeof(intptr_t)));
    }

    // Always align to cacheline boundary
    return static_cast<u32>(util::MathUtil::AlignTo(required_stack_space, CACHELINE_SIZE));
  }

  void Prologue() { push(rbx); }

  void Epilogue() { pop(rbx); }

  void AllocStack(u32 size) { sub(rsp, size); }

  void FreeStack(u32 size) { add(rsp, size); }

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

    const ast::FunctionType *func_type = func_.func_type();
    TPL_ASSERT(func_type->num_params() < sizeof(arg_regs), "Too many function arguments");

    u32 displacement = 0;
    u32 local_idx = 0;

    //
    // The first argument to the TBC function is a pointer to the return value.
    // If the function returns a non-void value, insert the pointer now.
    //

    if (const ast::Type *return_type = func_type->return_type(); !return_type->IsNilType()) {
      displacement = static_cast<u32>(util::MathUtil::AlignTo(return_type->size(), sizeof(intptr_t)));
      mov(ptr[rsp + displacement], rsp);
      local_idx++;
    }

    //
    // Now comes all the input arguments
    //

    for (u32 idx = 0; idx < func_type->num_params(); idx++, local_idx++) {
      const auto &local_info = func_.locals()[local_idx];
      auto use_64bit_reg = static_cast<u32>(local_info.size() > sizeof(u32));
      mov(ptr[rsp + displacement + local_info.offset()], arg_regs[use_64bit_reg][idx]);
    }
  }

  void InvokeVMFunction() {
    const ast::FunctionType *func_type = func_.func_type();
    const ast::Type *ret_type = func_type->return_type();
    u32 ret_type_size = 0;
    if (!ret_type->IsNilType()) {
      ret_type_size = static_cast<u32>(util::MathUtil::AlignTo(ret_type->size(), sizeof(intptr_t)));
    }

    // Set up the arguments to VM::InvokeFunction(module, function ID, args)
    mov(rdi, reinterpret_cast<std::size_t>(&module_));
    mov(rsi, func_.id());
    lea(rdx, ptr[rsp + ret_type_size]);

    // Call VM::InvokeFunction()
    mov(rax, reinterpret_cast<std::size_t>(&VM::InvokeFunction));
    call(rax);

    if (!ret_type->IsNilType()) {
      if (ret_type->size() < 8) {
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
  // Allocate memory
  std::error_code error;
  u32 flags = llvm::sys::Memory::ProtectionFlags::MF_READ | llvm::sys::Memory::ProtectionFlags::MF_WRITE;
  llvm::sys::MemoryBlock mem = llvm::sys::Memory::allocateMappedMemory(1 << 12, nullptr, flags, error);
  if (error) {
    EXECUTION_LOG_ERROR("There was an error allocating executable memory {}", error.message());
    return;
  }

  // Generate code
  TrampolineGenerator generator(*this, func, mem.base());
  generator.Generate();

  // Now that the code's been generated and finalized, let's remove write
  // protections and just make is read+exec.
  llvm::sys::Memory::protectMappedMemory(
      mem, llvm::sys::Memory::ProtectionFlags::MF_READ | llvm::sys::Memory::ProtectionFlags::MF_EXEC);

  // Done
  *trampoline = Trampoline(llvm::sys::OwningMemoryBlock(mem));
}

void Module::CreateFunctionTrampoline(FunctionId func_id) {
  // If a trampoline has already been setup, don't bother
  if (bytecode_trampolines_[func_id].code() != nullptr) {
    EXECUTION_LOG_DEBUG("Function {} has a trampoline; will not recreate", func_id);
    return;
  }

  // Lookup the function
  const auto *func_info = GetFuncInfoById(func_id);

  // Create the trampoline for the function
  Trampoline trampoline;
  CreateFunctionTrampoline(*func_info, &trampoline);

  // Mark available
  bytecode_trampolines_[func_id] = std::move(trampoline);
}

void Module::CompileToMachineCode() {
  std::call_once(compiled_flag_, [this]() {
    // If the module has already been compiled, nothing to do.
    if (jit_module_ != nullptr) {
      return;
    }

    // JIT
    LLVMEngine::CompilerOptions options;
    jit_module_ = LLVMEngine::Compile(*bytecode_module_, options);

    // Setup function pointers
    for (const auto &func_info : bytecode_module_->functions()) {
      auto *jit_function = jit_module_->GetFunctionPointer(func_info.name());
      TPL_ASSERT(jit_function != nullptr, "Missing function in compiled module!");
      functions_[func_info.id()].store(jit_function, std::memory_order_relaxed);
    }
  });
}

void Module::CompileToMachineCodeAsync() {
  auto *compile_task = new (tbb::task::allocate_root()) AsyncCompileTask(this);
  tbb::task::enqueue(*compile_task);
}

}  // namespace tpl::vm
