#include "execution/vm/bytecode_module.h"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#define XBYAK_NO_OP_NAMES
#include "xbyak/xbyak/xbyak.h"

#include "execution/ast/type.h"

namespace tpl::vm {

BytecodeModule::BytecodeModule(std::string name, std::vector<u8> &&code,
                               std::vector<FunctionInfo> &&functions)
    : name_(std::move(name)),
      code_(std::move(code)),
      functions_(std::move(functions)),
      trampolines_(functions_.size()) {
  // TODO(pmenon): Only create trampolines for exported functions
  for (const auto &func : functions_) {
    CreateFunctionTrampoline(func.id());
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
  TrampolineGenerator(const BytecodeModule &module,
                      const FunctionInfo &func_info, void *mem)
      : Xbyak::CodeGenerator(Xbyak::DEFAULT_MAX_CODE_SIZE, mem),
        module_(module),
        func_(func_info) {}

  /// Generate trampoline code for the given function in the given module
  void Generate() {
    // Compute the stack space needed for all arguments
    const u32 required_stack_space = ComputeRequiredStackSpace();

    // Allocate space for arguments
    AllocStack(required_stack_space);

    // Push call args onto stack
    PushCallerArgsOntoStack();

    // Invoke the VM entry function
    InvokeVMFunction();

    // Restore stack
    FreeStack(required_stack_space);

    // Return from the trampoline, placing return values where appropriate
    Return();
  }

 private:
  u32 ComputeRequiredStackSpace() const {
    // FunctionInfo tells us the amount of space we need for all input and
    // output arguments, so use that.
    u32 required_stack_space = static_cast<u32>(func_.params_size());

    // If the function has a return type, we need to allocate a temporary
    // return value on the stack for that as well. However, if the return type
    // is larger than 8 bytes (i.e., larger than a general-purpose register),
    // a pointer to the return value is provided to the trampoline as the first
    // argument
    const ast::Type *return_type = func_.func_type()->return_type();
    if (!return_type->IsNilType()) {
      required_stack_space +=
          static_cast<u32>(util::MathUtil::AlignTo(return_type->size(), sizeof(intptr_t)));
    }

    // Always align
    return static_cast<u32>(util::MathUtil::AlignTo(required_stack_space, sizeof(intptr_t)));
  }

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
    const Xbyak::Reg arg_regs[][6] = {{edi, esi, edx, ecx, r8d, r9d},
                                      {rdi, rsi, rdx, rcx, r8, r9}};

    const ast::FunctionType *func_type = func_.func_type();
    TPL_ASSERT(func_type->num_params() < sizeof(arg_regs),
               "Too many function arguments");

    u32 displacement = 0;
    u32 local_idx = 0;

    //
    // The first argument to the TBC function is a pointer to the return value.
    // If the function returns a non-void value, insert the pointer now.
    //

    if (const ast::Type *return_type = func_type->return_type();
        !return_type->IsNilType()) {
      displacement =
          static_cast<u32>(util::MathUtil::AlignTo(return_type->size(), sizeof(intptr_t)));
      mov(ptr[rsp + displacement], rsp);
      local_idx++;
    }

    //
    // Now comes all the input arguments
    //

    for (u32 idx = 0; idx < func_type->num_params(); idx++, local_idx++) {
      const auto &local_info = func_.locals()[local_idx];
      auto use_64bit_reg = static_cast<u32>(local_info.size() > sizeof(u32));
      mov(ptr[rsp + displacement + local_info.offset()],
          arg_regs[use_64bit_reg][idx]);
    }
  }

  void InvokeVMFunction() {
    const ast::FunctionType *func_type = func_.func_type();
    const u32 ret_type_size = static_cast<u32>(util::MathUtil::AlignTo(
        func_type->return_type()->size(), sizeof(intptr_t)));

    // Set up the arguments to VM::InvokeFunction(module, function ID, args)
    mov(rdi, reinterpret_cast<std::size_t>(&module_));
    mov(rsi, func_.id());
    lea(rdx, ptr[rsp + ret_type_size]);

    // Call VM::InvokeFunction()
    mov(rax, reinterpret_cast<std::size_t>(&VM::InvokeFunction));
    call(rax);

    if (const ast::Type *return_type = func_type->return_type();
        !return_type->IsNilType()) {
      if (return_type->size() < 8) {
        mov(eax, ptr[rsp]);
      } else {
        mov(rax, ptr[rsp]);
      }
    }
  }

  void Return() { ret(); }

 private:
  const BytecodeModule &module_;
  const FunctionInfo &func_;
};

}  // namespace

void BytecodeModule::CreateFunctionTrampoline(const FunctionInfo &func,
                                              Trampoline *trampoline) {
  // Allocate memory
  std::error_code error;
  u32 flags = llvm::sys::Memory::ProtectionFlags::MF_READ |
              llvm::sys::Memory::ProtectionFlags::MF_WRITE;
  llvm::sys::MemoryBlock mem =
      llvm::sys::Memory::allocateMappedMemory(1 << 12, nullptr, flags, error);
  if (error) {
    EXECUTION_LOG_ERROR("There was an error allocating executable memory {}",
              error.message());
    return;
  }

  // Generate code
  TrampolineGenerator generator(*this, func, mem.base());
  generator.Generate();

  // Now that the code's been generated and finalized, let's remove write
  // protections and just make is read+exec.
  llvm::sys::Memory::protectMappedMemory(
      mem, llvm::sys::Memory::ProtectionFlags::MF_READ |
               llvm::sys::Memory::ProtectionFlags::MF_EXEC);

  // Done
  *trampoline = Trampoline(llvm::sys::OwningMemoryBlock(mem));
}

void BytecodeModule::CreateFunctionTrampoline(FunctionId func_id) {
  // If a trampoline has already been setup, don't bother
  if (trampolines_[func_id].GetCode() != nullptr) {
    EXECUTION_LOG_DEBUG("Function {} has a trampoline; will not recreate", func_id);
    return;
  }

  // Lookup the function
  const auto *func_info = GetFuncInfoById(func_id);

  // Create the trampoline for the function
  Trampoline trampoline;
  CreateFunctionTrampoline(*func_info, &trampoline);

  // Mark available
  trampolines_[func_id] = std::move(trampoline);
}

namespace {

void PrettyPrintFuncInfo(std::ostream &os, const FunctionInfo &func) {
  os << "Function " << func.id() << " <" << func.name() << ">:" << std::endl;
  os << "  Frame size " << func.frame_size() << " bytes (" << func.num_params()
     << " parameter" << (func.num_params() > 1 ? "s, " : ", ")
     << func.locals().size() << " locals)" << std::endl;

  u64 max_local_len = 0;
  for (const auto &local : func.locals()) {
    max_local_len =
        std::max(max_local_len, static_cast<u64>(local.name().length()));
  }
  for (const auto &local : func.locals()) {
    if (local.is_parameter()) {
      os << "    param  ";
    } else {
      os << "    local  ";
    }
    os << std::setw(static_cast<int>(max_local_len)) << std::right << local.name()
       << ":  offset=" << std::setw(7) << std::left << local.offset()
       << " size=" << std::setw(7) << std::left << local.size()
       << " align=" << std::setw(7) << std::left << local.type()->alignment()
       << " type=" << std::setw(7) << std::left
       << ast::Type::ToString(local.type()) << std::endl;
  }
}

void PrettyPrintFuncCode(std::ostream &os, const FunctionInfo &func,
                         BytecodeIterator &iter) {
  const u32 max_inst_len = Bytecodes::MaxBytecodeNameLength();
  for (; !iter.Done(); iter.Advance()) {
    Bytecode bytecode = iter.CurrentBytecode();

    // Print common bytecode info
    os << "  0x" << std::right << std::setfill('0') << std::setw(8) << std::hex
       << iter.GetPosition();
    os << std::setfill(' ') << "    " << std::dec << std::setw(max_inst_len)
       << std::left << Bytecodes::ToString(bytecode) << std::endl;
  }
}

void PrettyPrintFunc(std::ostream &os, const BytecodeModule &module,
                     const FunctionInfo &func) {
  PrettyPrintFuncInfo(os, func);

  os << std::endl;

  auto iter = module.BytecodeForFunction(func);
  PrettyPrintFuncCode(os, func, iter);

  os << std::endl;
}

}  // namespace

void BytecodeModule::PrettyPrint(std::ostream &os) {
  for (const auto &func : functions_) {
    PrettyPrintFunc(os, *this, func);
  }
}

}  // namespace tpl::vm
