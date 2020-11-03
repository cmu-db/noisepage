#include "execution/vm/llvm_engine.h"

#include <llvm/ADT/StringMap.h>
#include <llvm/Analysis/TargetLibraryInfo.h>
#include <llvm/Analysis/TargetTransformInfo.h>
#include <llvm/Bitcode/BitcodeReader.h>
#include <llvm/ExecutionEngine/RuntimeDyld.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/Verifier.h>
#include <llvm/MC/MCContext.h>
#include <llvm/Support/DynamicLibrary.h>
#include <llvm/Support/Path.h>
#include <llvm/Support/SmallVectorMemoryBuffer.h>
#include <llvm/Support/TargetRegistry.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Target/TargetMachine.h>
#include <llvm/Transforms/IPO.h>
#include <llvm/Transforms/IPO/AlwaysInliner.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/ast/type.h"
#include "execution/vm/bytecode_module.h"
#include "execution/vm/bytecode_traits.h"
#include "loggers/execution_logger.h"

extern void *__dso_handle __attribute__((__visibility__("hidden")));  // NOLINT

namespace noisepage::execution::vm {

namespace {

bool FunctionHasIndirectReturn(const ast::FunctionType *func_type) {
  ast::Type *ret_type = func_type->GetReturnType();
  return (!ret_type->IsNilType() && ret_type->GetSize() > sizeof(int64_t));
}

bool FunctionHasDirectReturn(const ast::FunctionType *func_type) {
  ast::Type *ret_type = func_type->GetReturnType();
  return (!ret_type->IsNilType() && ret_type->GetSize() <= sizeof(int64_t));
}

}  // namespace

// ---------------------------------------------------------
// TPL's Jit Memory Manager
// ---------------------------------------------------------

class LLVMEngine::TPLMemoryManager : public llvm::SectionMemoryManager {
 public:
  llvm::JITSymbol findSymbol(const std::string &name) override {
    EXECUTION_LOG_TRACE("Resolving symbol '{}' ...", name);

    if (const auto iter = symbols_.find(name); iter != symbols_.end()) {
      EXECUTION_LOG_TRACE("Symbol '{}' found in cache ...", name);
      return llvm::JITSymbol(iter->second);
    }

    if (name == "__dso_handle") {
      EXECUTION_LOG_DEBUG("'__dso_handle' resolved to {} ...", reinterpret_cast<uint64_t>(&__dso_handle));
      return {reinterpret_cast<uint64_t>(&__dso_handle), {}};
    }

    EXECUTION_LOG_TRACE("Symbol '{}' not found in cache, checking process ...", name);

    llvm::JITSymbol symbol = llvm::SectionMemoryManager::findSymbol(name);
    // If you're here because you tripped the assertion, check that you have EXPORT'd symbol definitions.
    // Symptoms may include your code working in interpreted, adaptive, and codegen, but not in JIT.
#ifndef NDEBUG
    if (symbol.getAddress().get() == 0) {
      EXECUTION_LOG_ERROR("Resolved symbol has no address, you may need to EXPORT: {}", name);
    }
#endif  // NDEBUG
    NOISEPAGE_ASSERT(symbol.getAddress().get() != 0, "Resolved symbol has no address!");
    symbols_[name] = {symbol.getAddress().get(), symbol.getFlags()};
    return symbol;
  }

 private:
  std::unordered_map<std::string, llvm::JITEvaluatedSymbol> symbols_;
};

// ---------------------------------------------------------
// TPL Type to LLVM Type
// ---------------------------------------------------------

/** A handy class that maps TPL types to LLVM types. */
class LLVMEngine::TypeMap {
 public:
  explicit TypeMap(llvm::Module *module) : module_(module) {
    llvm::LLVMContext &ctx = module->getContext();
    type_map_["nil"] = llvm::Type::getVoidTy(ctx);
    type_map_["bool"] = llvm::Type::getInt8Ty(ctx);
    type_map_["int8"] = llvm::Type::getInt8Ty(ctx);
    type_map_["int16"] = llvm::Type::getInt16Ty(ctx);
    type_map_["int32"] = llvm::Type::getInt32Ty(ctx);
    type_map_["int64"] = llvm::Type::getInt64Ty(ctx);
    type_map_["int128"] = llvm::Type::getInt128Ty(ctx);
    type_map_["uint8"] = llvm::Type::getInt8Ty(ctx);
    type_map_["uint16"] = llvm::Type::getInt16Ty(ctx);
    type_map_["uint32"] = llvm::Type::getInt32Ty(ctx);
    type_map_["uint64"] = llvm::Type::getInt64Ty(ctx);
    type_map_["uint128"] = llvm::Type::getInt128Ty(ctx);
    type_map_["float32"] = llvm::Type::getFloatTy(ctx);
    type_map_["float64"] = llvm::Type::getDoubleTy(ctx);
    type_map_["string"] = llvm::Type::getInt8PtrTy(ctx);
  }

  /** No copying or moving this class. */
  DISALLOW_COPY_AND_MOVE(TypeMap);

  llvm::Type *VoidType() { return type_map_["nil"]; }
  llvm::Type *BoolType() { return type_map_["bool"]; }
  llvm::Type *Int8Type() { return type_map_["int8"]; }
  llvm::Type *Int16Type() { return type_map_["int16"]; }
  llvm::Type *Int32Type() { return type_map_["int32"]; }
  llvm::Type *Int64Type() { return type_map_["int64"]; }
  llvm::Type *UInt8Type() { return type_map_["uint8"]; }
  llvm::Type *UInt16Type() { return type_map_["uint16"]; }
  llvm::Type *UInt32Type() { return type_map_["uint32"]; }
  llvm::Type *UInt64Type() { return type_map_["uint64"]; }
  llvm::Type *Float32Type() { return type_map_["float32"]; }
  llvm::Type *Float64Type() { return type_map_["float64"]; }
  llvm::Type *StringType() { return type_map_["string"]; }

  llvm::Type *GetLLVMType(const ast::Type *type);

 private:
  // Given a non-primitive builtin type, convert it to an LLVM type
  llvm::Type *GetLLVMTypeForBuiltin(const ast::BuiltinType *builtin_type);

  // Given a struct type, convert it into an equivalent LLVM struct type
  llvm::StructType *GetLLVMStructType(const ast::StructType *struct_type);

  // Given a TPL function type, convert it into an equivalent LLVM function type
  llvm::FunctionType *GetLLVMFunctionType(const ast::FunctionType *func_type);

 private:
  llvm::Module *module_;
  std::unordered_map<std::string, llvm::Type *> type_map_;
};

llvm::Type *LLVMEngine::TypeMap::GetLLVMType(const ast::Type *type) {
  //
  // First, lookup the type in the cache. We do the lookup by performing a
  // try_emplace() in order to save a second lookup in the case when the type
  // does not exist in the cache. If the type is uncached, we can directly
  // update the returned iterator rather than performing another lookup.
  //

  auto [iter, inserted] = type_map_.try_emplace(type->ToString(), nullptr);

  if (!inserted) {
    return iter->second;
  }

  //
  // The type isn't cached, construct it now
  //

  llvm::Type *llvm_type = nullptr;
  switch (type->GetTypeId()) {
    case ast::Type::TypeId::StringType: {
      // These should be pre-filled in type cache!
      UNREACHABLE("Missing default type not found in cache");
    }
    case ast::Type::TypeId::BuiltinType: {
      llvm_type = GetLLVMTypeForBuiltin(type->As<ast::BuiltinType>());
      break;
    }
    case ast::Type::TypeId::PointerType: {
      auto *ptr_type = type->As<ast::PointerType>();
      llvm_type = llvm::PointerType::getUnqual(GetLLVMType(ptr_type->GetBase()));
      break;
    }
    case ast::Type::TypeId::ArrayType: {
      auto *arr_type = type->As<ast::ArrayType>();
      llvm::Type *elem_type = GetLLVMType(arr_type->GetElementType());
      if (arr_type->HasKnownLength()) {
        llvm_type = llvm::ArrayType::get(elem_type, arr_type->GetLength());
      } else {
        llvm_type = llvm::PointerType::getUnqual(elem_type);
      }
      break;
    }
    case ast::Type::TypeId::MapType: {
      // TODO(pmenon): me
      break;
    }
    case ast::Type::TypeId::StructType: {
      llvm_type = GetLLVMStructType(type->As<ast::StructType>());
      break;
    }
    case ast::Type::TypeId::FunctionType: {
      llvm_type = GetLLVMFunctionType(type->As<ast::FunctionType>());
      break;
    }
  }

  //
  // Update the cache with the constructed type
  //

  NOISEPAGE_ASSERT(llvm_type != nullptr, "No LLVM type found!");

  iter->second = llvm_type;

  return llvm_type;
}

llvm::Type *LLVMEngine::TypeMap::GetLLVMTypeForBuiltin(const ast::BuiltinType *builtin_type) {
  NOISEPAGE_ASSERT(!builtin_type->IsPrimitive(), "Primitive types should be cached!");

  // For the builtins, we perform a lookup using the C++ name
  const std::string name = builtin_type->GetCppName();

  // Try "struct" prefix
  if (llvm::Type *type = module_->getTypeByName("struct." + name)) {
    return type;
  }

  // Try "class" prefix
  if (llvm::Type *type = module_->getTypeByName("class." + name)) {
    return type;
  }

  EXECUTION_LOG_ERROR("Could not find LLVM type for TPL type '{}'", name);
  return nullptr;
}

llvm::StructType *LLVMEngine::TypeMap::GetLLVMStructType(const ast::StructType *struct_type) {
  // Collect the fields here
  llvm::SmallVector<llvm::Type *, 8> fields;

  for (const auto &field : struct_type->GetAllFields()) {
    fields.push_back(GetLLVMType(field.type_));
  }

  return llvm::StructType::create(fields);
}

llvm::FunctionType *LLVMEngine::TypeMap::GetLLVMFunctionType(const ast::FunctionType *func_type) {
  // Collect parameter types here
  llvm::SmallVector<llvm::Type *, 8> param_types;

  //
  // If the function has an indirect return value, insert it into the parameter
  // list as the hidden first argument, and setup the function to return void.
  // Otherwise, the return type of the LLVM function is the same as the TPL
  // function.
  //

  llvm::Type *return_type = nullptr;
  if (FunctionHasIndirectReturn(func_type)) {
    llvm::Type *rv_param = GetLLVMType(func_type->GetReturnType()->PointerTo());
    param_types.push_back(rv_param);
    // Return type of the function is void
    return_type = VoidType();
  } else {
    return_type = GetLLVMType(func_type->GetReturnType());
  }

  //
  // Now the formal parameters
  //

  for (const auto &param_info : func_type->GetParams()) {
    llvm::Type *param_type = GetLLVMType(param_info.type_);
    param_types.push_back(param_type);
  }

  return llvm::FunctionType::get(return_type, param_types, false);
}

// ---------------------------------------------------------
// Function Locals Map
// ---------------------------------------------------------

/** This class provides access to a function's local variables. */
class LLVMEngine::FunctionLocalsMap {
 public:
  FunctionLocalsMap(const FunctionInfo &func_info, llvm::Function *func, TypeMap *type_map,
                    llvm::IRBuilder<> *ir_builder);

  // Given a reference to a local variable in a function's local variable list,
  // return the corresponding LLVM value.
  llvm::Value *GetArgumentById(LocalVar var);

 private:
  llvm::IRBuilder<> *ir_builder_;
  llvm::DenseMap<uint32_t, llvm::Value *> params_;
  llvm::DenseMap<uint32_t, llvm::Value *> locals_;
};

LLVMEngine::FunctionLocalsMap::FunctionLocalsMap(const FunctionInfo &func_info, llvm::Function *func, TypeMap *type_map,
                                                 llvm::IRBuilder<> *ir_builder)
    : ir_builder_(ir_builder) {
  uint32_t local_idx = 0;

  const auto &func_locals = func_info.GetLocals();

  // Make an allocation for the return value, if it's direct.
  if (const ast::FunctionType *func_type = func_info.GetFuncType(); FunctionHasDirectReturn(func_type)) {
    llvm::Type *ret_type = type_map->GetLLVMType(func_type->GetReturnType());
    llvm::Value *val = ir_builder_->CreateAlloca(ret_type);
    params_[func_locals[0].GetOffset()] = val;
    local_idx++;
  }

  // Cache parameters.
  for (auto arg_iter = func->arg_begin(); local_idx < func_info.GetParamsCount(); ++local_idx, ++arg_iter) {
    const LocalInfo &param = func_locals[local_idx];
    params_[param.GetOffset()] = &*arg_iter;
  }

  // Allocate all local variables up front.
  for (; local_idx < func_info.GetLocals().size(); local_idx++) {
    const LocalInfo &local_info = func_locals[local_idx];
    llvm::Type *llvm_type = type_map->GetLLVMType(local_info.GetType());
    llvm::Value *val = ir_builder_->CreateAlloca(llvm_type);
    locals_[local_info.GetOffset()] = val;
  }
}

llvm::Value *LLVMEngine::FunctionLocalsMap::GetArgumentById(LocalVar var) {
  if (auto iter = params_.find(var.GetOffset()); iter != params_.end()) {
    return iter->second;
  }

  if (auto iter = locals_.find(var.GetOffset()); iter != locals_.end()) {
    llvm::Value *val = iter->second;

    if (var.GetAddressMode() == LocalVar::AddressMode::Value) {
      val = ir_builder_->CreateLoad(val);
    }

    return val;
  }

  EXECUTION_LOG_ERROR("No variable found at offset {}", var.GetOffset());

  return nullptr;
}

// ---------------------------------------------------------
// Compiled Module Builder
// ---------------------------------------------------------

/** A builder for compiled modules. We need this because compiled modules are immutable after creation. */
class LLVMEngine::CompiledModuleBuilder {
 public:
  CompiledModuleBuilder(const CompilerOptions &options, const BytecodeModule &tpl_module);

  // No copying or moving this class
  DISALLOW_COPY_AND_MOVE(CompiledModuleBuilder);

  // Generate all static local data in the module
  void DeclareStaticLocals();

  // Generate function declarations for each function in the TPL bytecode module
  void DeclareFunctions();

  // Generate an LLVM function implementation for each function defined in the
  // TPL bytecode module. DeclareFunctions() must be called to generate function
  // declarations before they can be defined.
  void DefineFunctions();

  // Verify that all generated code is good
  void Verify();

  // Remove unused code to make optimizations quicker
  void Simplify();

  // Optimize the generate code
  void Optimize();

  // Perform finalization logic and create a compiled module
  std::unique_ptr<CompiledModule> Finalize();

  // Print the contents of the module to a string and return it
  std::string DumpModuleIR();

  // Print the contents of the module's assembly to a string and return it
  std::string DumpModuleAsm();

 private:
  // Given a TPL function, build a simple CFG using 'blocks' as an output param
  void BuildSimpleCFG(const FunctionInfo &func_info, std::map<std::size_t, llvm::BasicBlock *> *blocks);

  // Convert one TPL function into an LLVM implementation
  void DefineFunction(const FunctionInfo &func_info, llvm::IRBuilder<> *ir_builder);

  // Given a bytecode, lookup it's LLVM function handler in the module
  llvm::Function *LookupBytecodeHandler(Bytecode bytecode) const;

  // Generate an in-memory shared object from this LLVM module. It iss assumed
  // that all functions have been generated and verified.
  std::unique_ptr<llvm::MemoryBuffer> EmitObject();

  // Write the given object to the file system
  void PersistObjectToFile(const llvm::MemoryBuffer &obj_buffer);

 private:
  const CompilerOptions &options_;
  const BytecodeModule &tpl_module_;
  std::unique_ptr<llvm::TargetMachine> target_machine_;
  std::unique_ptr<llvm::LLVMContext> context_;
  std::unique_ptr<llvm::Module> llvm_module_;
  std::unique_ptr<TypeMap> type_map_;
  llvm::DenseMap<std::size_t, llvm::Constant *> static_locals_;
};

// ---------------------------------------------------------
// Compiled Module Builder
// ---------------------------------------------------------

LLVMEngine::CompiledModuleBuilder::CompiledModuleBuilder(const CompilerOptions &options,
                                                         const BytecodeModule &tpl_module)
    : options_(options),
      tpl_module_(tpl_module),
      target_machine_(nullptr),
      context_(std::make_unique<llvm::LLVMContext>()),
      llvm_module_(nullptr),
      type_map_(nullptr) {
  //
  // We need to create a suitable TargetMachine for LLVM to before we can JIT
  // TPL programs. At the moment, we rely on LLVM to discover all CPU features
  // e.g., AVX2 or AVX512, and we make no assumptions about symbol relocations.
  //
  // TODO(pmenon): This may change with LLVM8 that comes with
  // TargetMachineBuilders
  // TODO(pmenon): Alter the flags as need be
  //

  const std::string target_triple = llvm::sys::getProcessTriple();

  {
    std::string error;
    auto *target = llvm::TargetRegistry::lookupTarget(target_triple, error);
    if (target == nullptr) {
      EXECUTION_LOG_ERROR("LLVM: Unable to find target with target_triple {}", target_triple);
      return;
    }

    // Collect CPU features
    llvm::StringMap<bool> feature_map;
    if (bool success = llvm::sys::getHostCPUFeatures(feature_map); !success) {
      EXECUTION_LOG_ERROR("LLVM: Unable to find all CPU features");
      return;
    }

    llvm::SubtargetFeatures target_features;
    for (const auto &entry : feature_map) {
      target_features.AddFeature(entry.getKey(), entry.getValue());
    }

    EXECUTION_LOG_TRACE("LLVM: Discovered CPU features: {}", target_features.getString());

    // Both relocation=PIC or JIT=true work. Use the latter for now.
    llvm::TargetOptions target_options;
    llvm::Optional<llvm::Reloc::Model> reloc;
    const llvm::CodeGenOpt::Level opt_level = llvm::CodeGenOpt::Aggressive;
    target_machine_.reset(target->createTargetMachine(target_triple, llvm::sys::getHostCPUName(),
                                                      target_features.getString(), target_options, reloc, {}, opt_level,
                                                      true));
    NOISEPAGE_ASSERT(target_machine_ != nullptr, "LLVM: Unable to find a suitable target machine!");
  }

  //
  // We've built a TargetMachine we use to generate machine code. Now, we
  // load the pre-compiled bytecode module containing all the TPL bytecode
  // logic. We add the functions we're about to compile into this module. This
  // module forms the unit of JIT.
  //

  {
    auto memory_buffer = llvm::MemoryBuffer::getFile(options.GetBytecodeHandlersBcPath());
    if (auto error = memory_buffer.getError()) {
      EXECUTION_LOG_ERROR("There was an error loading the handler bytecode: {}", error.message());
    }

    auto module = llvm::parseBitcodeFile(*(memory_buffer.get()), *context_);
    if (!module) {
      auto error = llvm::toString(module.takeError());
      EXECUTION_LOG_ERROR("{}", error);
      throw std::runtime_error(error);
    }

    llvm_module_ = std::move(module.get());
    llvm_module_->setModuleIdentifier(tpl_module.GetName());
    llvm_module_->setSourceFileName(tpl_module.GetName() + ".tpl");
    llvm_module_->setDataLayout(target_machine_->createDataLayout());
    llvm_module_->setTargetTriple(target_triple);
  }

  type_map_ = std::make_unique<TypeMap>(llvm_module_.get());
}

void LLVMEngine::CompiledModuleBuilder::DeclareStaticLocals() {
  for (const auto &local_info : tpl_module_.GetStaticLocalsInfo()) {
    // The raw data wrapped in a string reference
    llvm::StringRef data_ref(
        reinterpret_cast<const char *>(tpl_module_.AccessStaticLocalDataRaw(local_info.GetOffset())),
        local_info.GetSize());

    // The constant
    llvm::Constant *string_constant = llvm::ConstantDataArray::getString(*context_, data_ref);

    // The global variable wrapping the constant. It's private to this module, so it does NOT
    // participate in linking. It also not thread-local since it's shared across all threads. Don't
    // be alarmed by 'new' here; the module will take care of deleting it.
    auto *global_var =
        new llvm::GlobalVariable(*llvm_module_, string_constant->getType(), true, llvm::GlobalValue::PrivateLinkage,
                                 string_constant, local_info.GetName(), nullptr, llvm::GlobalVariable::NotThreadLocal);
    global_var->setUnnamedAddr(llvm::GlobalValue::UnnamedAddr::Global);
    global_var->setAlignment(1);

    // Convert the global variable into an i8*
    llvm::Constant *zero = llvm::ConstantInt::get(type_map_->Int32Type(), 0);
    llvm::Constant *indices[] = {zero, zero};
    auto result = llvm::ConstantExpr::getInBoundsGetElementPtr(global_var->getValueType(), global_var, indices);

    // Cache
    static_locals_[local_info.GetOffset()] = result;
  }
}

void LLVMEngine::CompiledModuleBuilder::DeclareFunctions() {
  for (const auto &func_info : tpl_module_.GetFunctionsInfo()) {
    auto *func_type = llvm::cast<llvm::FunctionType>(type_map_->GetLLVMType(func_info.GetFuncType()));
    llvm_module_->getOrInsertFunction(func_info.GetName(), func_type);
  }
}

llvm::Function *LLVMEngine::CompiledModuleBuilder::LookupBytecodeHandler(Bytecode bytecode) const {
  const char *handler_name = Bytecodes::GetBytecodeHandlerName(bytecode);
  llvm::Function *func = llvm_module_->getFunction(handler_name);
#ifndef NDEBUG
  if (func == nullptr) {
    auto error =
        fmt::format("No bytecode handler function '{}' for bytecode {}", handler_name, Bytecodes::ToString(bytecode));
    EXECUTION_LOG_ERROR("{}", error);
    throw std::runtime_error(error);
  }
#endif
  return func;
}

void LLVMEngine::CompiledModuleBuilder::BuildSimpleCFG(const FunctionInfo &func_info,
                                                       std::map<std::size_t, llvm::BasicBlock *> *blocks) {
  // Before we can generate LLVM IR, we need to build a control-flow graph (CFG) for the function.
  // We do this construction directly from the TPL bytecode using a vanilla DFS and produce an
  // ordered map ('blocks') from bytecode position to an LLVM basic block. Each entry in the map
  // indicates the start of a basic block.

  // We use this vector as a stack for DFS traversal
  llvm::SmallVector<std::size_t, 16> bb_begin_positions = {0};

  for (auto iter = tpl_module_.GetBytecodeForFunction(func_info); !bb_begin_positions.empty();) {
    std::size_t begin_pos = bb_begin_positions.back();
    bb_begin_positions.pop_back();

    // We're at what we think is the start of a new basic block. Scan it until we find a terminal
    // instruction. Once we do,
    for (iter.SetPosition(begin_pos); !iter.Done(); iter.Advance()) {
      Bytecode bytecode = iter.CurrentBytecode();

      // If the bytecode isn't a terminal for the block, continue until we reach one
      if (!Bytecodes::IsTerminal(bytecode)) {
        continue;
      }

      // Return?
      if (Bytecodes::IsReturn(bytecode)) {
        break;
      }

      // Unconditional branch?
      if (Bytecodes::IsUnconditionalJump(bytecode)) {
        std::size_t branch_target_pos =
            iter.GetPosition() + Bytecodes::GetNthOperandOffset(bytecode, 0) + iter.GetJumpOffsetOperand(0);

        if (blocks->find(branch_target_pos) == blocks->end()) {
          (*blocks)[branch_target_pos] = nullptr;
          bb_begin_positions.push_back(branch_target_pos);
        }

        break;
      }

      // Conditional branch?
      if (Bytecodes::IsConditionalJump(bytecode)) {
        std::size_t fallthrough_pos = iter.GetPosition() + iter.CurrentBytecodeSize();

        if (blocks->find(fallthrough_pos) == blocks->end()) {
          bb_begin_positions.push_back(fallthrough_pos);
          (*blocks)[fallthrough_pos] = nullptr;
        }

        std::size_t branch_target_pos =
            iter.GetPosition() + Bytecodes::GetNthOperandOffset(bytecode, 1) + iter.GetJumpOffsetOperand(1);

        if (blocks->find(branch_target_pos) == blocks->end()) {
          bb_begin_positions.push_back(branch_target_pos);
          (*blocks)[branch_target_pos] = nullptr;
        }

        break;
      }
    }
  }
}

void LLVMEngine::CompiledModuleBuilder::DefineFunction(const FunctionInfo &func_info, llvm::IRBuilder<> *ir_builder) {
  llvm::LLVMContext &ctx = ir_builder->getContext();
  llvm::Function *func = llvm_module_->getFunction(func_info.GetName());
  llvm::BasicBlock *first_bb = llvm::BasicBlock::Create(ctx, "BB0", func);
  llvm::BasicBlock *entry_bb = llvm::BasicBlock::Create(ctx, "EntryBB", func, first_bb);

  // First, construct a simple CFG for the function. The CFG contains entries for the start of every
  // basic block in the function, and the bytecode position of the first instruction in the block.
  // The CFG is ordered by bytecode position in ascending order.

  std::map<std::size_t, llvm::BasicBlock *> blocks = {{0, first_bb}};
  BuildSimpleCFG(func_info, &blocks);

  {
    uint32_t block_num = 1;
    for (auto &[_, block] : blocks) {
      (void)_;
      if (block == nullptr) {
        block = llvm::BasicBlock::Create(ctx, "BB" + std::to_string(block_num++), func);
      }
    }
  }

#ifndef NDEBUG
  EXECUTION_LOG_TRACE("Found blocks:");
  for (auto &[pos, block] : blocks) {
    (void)pos;
    (void)block;
    EXECUTION_LOG_TRACE("  Block {} @ {:x}", block->getName().str(), pos);
  }
#endif

  // We can define the function now. LLVM IR generation happens by iterating over the function's
  // bytecode simultaneously with the ordered list of basic block start positions ('blocks'). Each
  // TPL bytecode is converted to a function call into a pre-compiled TPL bytecode handler. However,
  // many of these calls will get inlined away during optimization. If the current bytecode position
  // matches the position of a new basic block, a branch instruction is generated automatically
  // (either conditional or not depending on context) into the new block, and the IR builder
  // position shifts to the new block.

  ir_builder->SetInsertPoint(entry_bb);

  FunctionLocalsMap locals_map(func_info, func, type_map_.get(), ir_builder);

  // Jump to the first block, after all local allocations have been made.
  ir_builder->CreateBr(first_bb);
  ir_builder->SetInsertPoint(first_bb);

  for (auto iter = tpl_module_.GetBytecodeForFunction(func_info); !iter.Done(); iter.Advance()) {
    Bytecode bytecode = iter.CurrentBytecode();

    // Collect arguments
    llvm::SmallVector<llvm::Value *, 8> args;
    for (uint32_t i = 0; i < Bytecodes::NumOperands(bytecode); i++) {
      switch (Bytecodes::GetNthOperandType(bytecode, i)) {
        case OperandType::None: {
          break;
        }
        case OperandType::Imm1: {
          args.push_back(llvm::ConstantInt::get(type_map_->Int8Type(), iter.GetImmediateIntegerOperand(i), true));
          break;
        }
        case OperandType::Imm2: {
          args.push_back(llvm::ConstantInt::get(type_map_->Int16Type(), iter.GetImmediateIntegerOperand(i), true));
          break;
        }
        case OperandType::Imm4: {
          args.push_back(llvm::ConstantInt::get(type_map_->Int32Type(), iter.GetImmediateIntegerOperand(i), true));
          break;
        }
        case OperandType::Imm8: {
          args.push_back(llvm::ConstantInt::get(type_map_->Int64Type(), iter.GetImmediateIntegerOperand(i), true));
          break;
        }
        case OperandType::Imm4F: {
          args.push_back(llvm::ConstantFP::get(type_map_->Float32Type(), iter.GetImmediateFloatOperand(i)));
          break;
        }
        case OperandType::Imm8F: {
          args.push_back(llvm::ConstantFP::get(type_map_->Float64Type(), iter.GetImmediateFloatOperand(i)));
          break;
        }
        case OperandType::UImm2: {
          args.push_back(
              llvm::ConstantInt::get(type_map_->UInt16Type(), iter.GetUnsignedImmediateIntegerOperand(i), false));
          break;
        }
        case OperandType::UImm4: {
          args.push_back(
              llvm::ConstantInt::get(type_map_->UInt32Type(), iter.GetUnsignedImmediateIntegerOperand(i), false));
          break;
        }
        case OperandType::FunctionId: {
          const FunctionInfo *target_func_info = tpl_module_.GetFuncInfoById(iter.GetFunctionIdOperand(i));
          llvm::Function *target_func = llvm_module_->getFunction(target_func_info->GetName());
          NOISEPAGE_ASSERT(target_func != nullptr, "Function doesn't exist in LLVM module");
          args.push_back(target_func);
          break;
        }
        case OperandType::JumpOffset: {
          // These are handled specially below
          break;
        }
        case OperandType::Local: {
          LocalVar local = iter.GetLocalOperand(i);
          args.push_back(locals_map.GetArgumentById(local));
          break;
        }
        case OperandType::StaticLocal: {
          const uint32_t offset = iter.GetStaticLocalOperand(i).GetOffset();
          const auto static_locals_iter = static_locals_.find(offset);
          NOISEPAGE_ASSERT(static_locals_iter != static_locals_.end(), "Static local at offset does not exist");
          args.push_back(static_locals_iter->second);
          break;
        }
        case OperandType::LocalCount: {
          std::vector<LocalVar> locals;
          iter.GetLocalCountOperand(i, &locals);
          for (const auto local : locals) {
            args.push_back(locals_map.GetArgumentById(local));
          }
          break;
        }
      }
    }

    const auto issue_call = [&ir_builder](auto *func, auto &args) {
      auto arg_iter = func->arg_begin();
      for (uint32_t i = 0; i < args.size(); ++i, ++arg_iter) {
        llvm::Type *expected_type = arg_iter->getType();
        llvm::Type *provided_type = args[i]->getType();

        if (provided_type == expected_type) {
          continue;
        }

        if (expected_type->isIntegerTy()) {
          if (provided_type->isPointerTy()) {
            args[i] = ir_builder->CreatePtrToInt(args[i], expected_type);
          } else {
            args[i] = ir_builder->CreateIntCast(args[i], expected_type, true);
          }
        } else if (expected_type->isPointerTy()) {
          NOISEPAGE_ASSERT(provided_type->isPointerTy(), "Mismatched types");
          args[i] = ir_builder->CreateBitCast(args[i], expected_type);
        }
      }
      return ir_builder->CreateCall(func, args);
    };

    // Handle bytecode
    switch (bytecode) {
      case Bytecode::Call: {
        // For internal calls, the callee's function ID will be the first operand. We pull it out
        // and lookup the function in the module and remove it from the arguments vector.
        //
        // If the function has a direct return, the second operand will be the value to store the
        // result of the invocation into. We pop it off the argument vector, issue the call, then
        // store the result. The remaining elements are legitimate arguments to the function.

        const FunctionId callee_id = iter.GetFunctionIdOperand(0);
        const auto *callee_func_info = tpl_module_.GetFuncInfoById(callee_id);
        llvm::Function *callee = llvm_module_->getFunction(callee_func_info->GetName());
        args.erase(args.begin());

        if (FunctionHasDirectReturn(callee_func_info->GetFuncType())) {
          llvm::Value *dest = args[0];
          args.erase(args.begin());
          llvm::Value *ret = issue_call(callee, args);
          ir_builder->CreateStore(ret, dest);
        } else {
          issue_call(callee, args);
        }

        break;
      }

      case Bytecode::Jump: {
        // Unconditional jumps work as follows: we read the relative target bytecode position from
        // the iterator, calculate the absolute bytecode position, and create an unconditional
        // branch to the basic block that starts at the given bytecode position, using the
        // information in the CFG.

        std::size_t branch_target_bb_pos =
            iter.GetPosition() + Bytecodes::GetNthOperandOffset(bytecode, 0) + iter.GetJumpOffsetOperand(0);
        NOISEPAGE_ASSERT(blocks[branch_target_bb_pos] != nullptr, "Branch target does not point to valid basic block");
        ir_builder->CreateBr(blocks[branch_target_bb_pos]);
        break;
      }

      case Bytecode::JumpIfFalse:
      case Bytecode::JumpIfTrue: {
        // Conditional jumps work almost exactly as unconditional jump except a second fallthrough
        // position is calculated.

        std::size_t fallthrough_bb_pos = iter.GetPosition() + iter.CurrentBytecodeSize();
        std::size_t branch_target_bb_pos =
            iter.GetPosition() + Bytecodes::GetNthOperandOffset(bytecode, 1) + iter.GetJumpOffsetOperand(1);
        NOISEPAGE_ASSERT(blocks[fallthrough_bb_pos] != nullptr,
                         "Branch fallthrough does not point to valid basic block");
        NOISEPAGE_ASSERT(blocks[branch_target_bb_pos] != nullptr, "Branch target does not point to valid basic block");

        auto *check = llvm::ConstantInt::get(type_map_->Int8Type(), 1, false);
        llvm::Value *cond = ir_builder->CreateICmpEQ(args[0], check);

        if (bytecode == Bytecode::JumpIfTrue) {
          ir_builder->CreateCondBr(cond, blocks[branch_target_bb_pos], blocks[fallthrough_bb_pos]);
        } else {
          ir_builder->CreateCondBr(cond, blocks[fallthrough_bb_pos], blocks[branch_target_bb_pos]);
        }
        break;
      }

      case Bytecode::Lea: {
        NOISEPAGE_ASSERT(args[1]->getType()->isPointerTy(), "First argument must be a pointer");
        const llvm::DataLayout &dl = llvm_module_->getDataLayout();
        llvm::Type *pointee_type = args[1]->getType()->getPointerElementType();
        int64_t offset = llvm::cast<llvm::ConstantInt>(args[2])->getSExtValue();
        if (auto struct_type = llvm::dyn_cast<llvm::StructType>(pointee_type)) {
          const uint32_t elem_index = dl.getStructLayout(struct_type)->getElementContainingOffset(offset);
          llvm::Value *addr = ir_builder->CreateStructGEP(args[1], elem_index);
          ir_builder->CreateStore(addr, args[0]);
        } else {
          llvm::SmallVector<llvm::Value *, 2> gep_args;
          uint32_t elem_size = dl.getTypeSizeInBits(pointee_type);
          if (llvm::isa<llvm::ArrayType>(pointee_type)) {
            llvm::Type *const arr_type = pointee_type->getArrayElementType();
            elem_size = dl.getTypeSizeInBits(arr_type) / common::Constants::K_BITS_PER_BYTE;
            gep_args.push_back(llvm::ConstantInt::get(type_map_->Int64Type(), 0));
          }
          gep_args.push_back(llvm::ConstantInt::get(type_map_->Int64Type(), offset / elem_size));
          llvm::Value *addr = ir_builder->CreateInBoundsGEP(args[1], gep_args);
          ir_builder->CreateStore(addr, args[0]);
        }

        break;
      }

      case Bytecode::LeaScaled: {
        NOISEPAGE_ASSERT(args[1]->getType()->isPointerTy(), "First argument must be a pointer");
        // For any LeaScaled, the scale is handled by LLVM's GEP. If it's an
        // array of structures, we need to handle the final offset/displacement
        // into the struct as the last GEP index.
        llvm::Type *pointee_type = args[1]->getType()->getPointerElementType();
        if (auto struct_type = llvm::dyn_cast<llvm::StructType>(pointee_type)) {
          llvm::Value *addr = ir_builder->CreateInBoundsGEP(args[1], {args[2]});
          const uint64_t elem_offset = llvm::cast<llvm::ConstantInt>(args[4])->getZExtValue();
          if (elem_offset != 0) {
            const uint32_t elem_index =
                llvm_module_->getDataLayout().getStructLayout(struct_type)->getElementContainingOffset(elem_offset);
            addr = ir_builder->CreateStructGEP(addr, elem_index);
          }
          ir_builder->CreateStore(addr, args[0]);
        } else {
          NOISEPAGE_ASSERT(llvm::cast<llvm::ConstantInt>(args[4])->getSExtValue() == 0,
                           "LeaScaled on arrays cannot have a displacement");
          llvm::SmallVector<llvm::Value *, 2> gep_args;
          if (llvm::isa<llvm::ArrayType>(pointee_type)) {
            gep_args.push_back(llvm::ConstantInt::get(type_map_->Int64Type(), 0));
          }
          gep_args.push_back(args[2]);
          llvm::Value *addr = ir_builder->CreateInBoundsGEP(args[1], gep_args);
          ir_builder->CreateStore(addr, args[0]);
        }
        break;
      }

      case Bytecode::Return: {
        if (FunctionHasDirectReturn(func_info.GetFuncType())) {
          llvm::Value *ret_val = locals_map.GetArgumentById(func_info.GetReturnValueLocal());
          ir_builder->CreateRet(ir_builder->CreateLoad(ret_val));
        } else {
          ir_builder->CreateRetVoid();
        }
        break;
      }

      default: {
        // In the default case, each bytecode makes a function call into its bytecode handler
        llvm::Function *handler = LookupBytecodeHandler(bytecode);
        issue_call(handler, args);
        break;
      }
    }

    // If the next bytecode marks the start of a new basic block (based on the CFG), we need to
    // switch insertion points to it before continuing IR generation. But, it could be the case that
    // we're starting a new block

    auto next_bytecode_pos = iter.GetPosition() + iter.CurrentBytecodeSize();

    if (auto blocks_iter = blocks.find(next_bytecode_pos); blocks_iter != blocks.end()) {
      if (!Bytecodes::IsTerminal(bytecode)) {
        ir_builder->CreateBr(blocks_iter->second);
      }
      ir_builder->SetInsertPoint(blocks_iter->second);
    }
  }
}

void LLVMEngine::CompiledModuleBuilder::DefineFunctions() {
  llvm::IRBuilder<> ir_builder(*context_);
  for (const auto &func_info : tpl_module_.GetFunctionsInfo()) {
    DefineFunction(func_info, &ir_builder);
  }
}

void LLVMEngine::CompiledModuleBuilder::Verify() {
  std::string result;
  llvm::raw_string_ostream ostream(result);
  if (bool has_error = llvm::verifyModule(*llvm_module_, &ostream); has_error) {
    // TODO(pmenon): Do something more here ...
    EXECUTION_LOG_ERROR("ERROR IN MODULE:\n{}", ostream.str());
  }
}

void LLVMEngine::CompiledModuleBuilder::Simplify() {
  // When this function is called, the generated IR consists of many function
  // calls to cross-compiled bytecode handler functions. We now inline those
  // function calls directly into the body of the functions we've generated
  // by running the 'AlwaysInliner' pass.
  llvm::legacy::PassManager pass_manager;
  pass_manager.add(llvm::createAlwaysInlinerLegacyPass());
  pass_manager.add(llvm::createGlobalDCEPass());
  pass_manager.run(*llvm_module_);
}

void LLVMEngine::CompiledModuleBuilder::Optimize() {
  llvm::legacy::PassManager module_passes;
  llvm::legacy::FunctionPassManager function_passes(llvm_module_.get());

  // Add the appropriate TargetLibraryInfo and TargetTransformInfo
  auto target_library_info_impl = std::make_unique<llvm::TargetLibraryInfoImpl>(target_machine_->getTargetTriple());
  function_passes.add(llvm::createTargetTransformInfoWrapperPass(target_machine_->getTargetIRAnalysis()));
  module_passes.add(new llvm::TargetLibraryInfoWrapperPass(*target_library_info_impl));
  module_passes.add(llvm::createTargetTransformInfoWrapperPass(target_machine_->getTargetIRAnalysis()));

  // Build up optimization pipeline
  llvm::PassManagerBuilder pm_builder;
  pm_builder.OptLevel = 3;
  pm_builder.Inliner = llvm::createFunctionInliningPass(3, 0, false);
  pm_builder.populateFunctionPassManager(function_passes);
  pm_builder.populateModulePassManager(module_passes);

  // Run optimization passes on module
  function_passes.doInitialization();
  for (auto &func : *llvm_module_) {
    function_passes.run(func);
  }
  function_passes.doFinalization();
  module_passes.run(*llvm_module_);
}

std::unique_ptr<LLVMEngine::CompiledModule> LLVMEngine::CompiledModuleBuilder::Finalize() {
  std::unique_ptr<llvm::MemoryBuffer> obj = EmitObject();

  if (options_.ShouldPersistObjectFile()) {
    PersistObjectToFile(*obj);
  }

  return std::make_unique<CompiledModule>(std::move(obj));
}

std::unique_ptr<llvm::MemoryBuffer> LLVMEngine::CompiledModuleBuilder::EmitObject() {
  // Buffer holding the machine code. The returned buffer will take ownership of
  // this one when we return
  llvm::SmallString<4096> obj_buffer;

  // The pass manager we insert the EmitMC pass into
  llvm::legacy::PassManager pass_manager;
  pass_manager.add(new llvm::TargetLibraryInfoWrapperPass(target_machine_->getTargetTriple()));
  pass_manager.add(llvm::createTargetTransformInfoWrapperPass(target_machine_->getTargetIRAnalysis()));

  llvm::MCContext *mc_ctx;
  llvm::raw_svector_ostream obj_buffer_stream(obj_buffer);
  if (target_machine_->addPassesToEmitMC(pass_manager, mc_ctx, obj_buffer_stream)) {
    EXECUTION_LOG_ERROR("The target LLVM machine cannot emit a file of this type");
    return nullptr;
  }

  // Generate code
  pass_manager.run(*llvm_module_);

  return std::make_unique<llvm::SmallVectorMemoryBuffer>(std::move(obj_buffer));
}

void LLVMEngine::CompiledModuleBuilder::PersistObjectToFile(const llvm::MemoryBuffer &obj_buffer) {
  const std::string file_name = tpl_module_.GetName() + ".to";

  std::error_code error_code;
  llvm::raw_fd_ostream dest(file_name, error_code, llvm::sys::fs::F_None);

  if (error_code) {
    EXECUTION_LOG_ERROR("Could not open file: {}", error_code.message());
    return;
  }

  dest.write(obj_buffer.getBufferStart(), obj_buffer.getBufferSize());
  dest.flush();
  dest.close();
}

std::string LLVMEngine::CompiledModuleBuilder::DumpModuleIR() {
  std::string result;
  llvm::raw_string_ostream ostream(result);
  llvm_module_->print(ostream, nullptr);
  return result;
}

std::string LLVMEngine::CompiledModuleBuilder::DumpModuleAsm() {
  llvm::SmallString<1024> asm_str;
  llvm::raw_svector_ostream ostream(asm_str);

  llvm::legacy::PassManager pass_manager;
  pass_manager.add(llvm::createTargetTransformInfoWrapperPass(target_machine_->getTargetIRAnalysis()));
  target_machine_->Options.MCOptions.AsmVerbose = true;
  target_machine_->addPassesToEmitFile(pass_manager, ostream, nullptr, llvm::TargetMachine::CGFT_AssemblyFile);
  pass_manager.run(*llvm_module_);
  target_machine_->Options.MCOptions.AsmVerbose = false;

  return asm_str.str().str();
}

// ---------------------------------------------------------
// Compiled Module
// ---------------------------------------------------------

LLVMEngine::CompiledModule::CompiledModule(std::unique_ptr<llvm::MemoryBuffer> object_code)
    : loaded_(false),
      object_code_(std::move(object_code)),
      memory_manager_(std::make_unique<LLVMEngine::TPLMemoryManager>()) {}

// This destructor is needed to address a bug with LLVM's RuntimeDyldElf.
// See issue #961 and PR #962 for further information.
LLVMEngine::CompiledModule::~CompiledModule() { memory_manager_->deregisterEHFrames(); }

void *LLVMEngine::CompiledModule::GetFunctionPointer(const std::string &name) const {
  NOISEPAGE_ASSERT(IsLoaded(), "Compiled module isn't loaded!");

  if (auto iter = functions_.find(name); iter != functions_.end()) {
    return iter->second;
  }

  return nullptr;
}

void LLVMEngine::CompiledModule::Load(const BytecodeModule &module) {
  // If already loaded, do nothing
  if (IsLoaded()) {
    return;
  }

  //
  // CompiledModules can be created with or without an in-memory object file. If
  // this one was created without an in-memory object file, we need to load it
  // from the file system. We use the module's name to find it in the current
  // directory.
  //

  if (object_code_ == nullptr) {
    llvm::SmallString<128> path;
    if (std::error_code error = llvm::sys::fs::current_path(path)) {
      EXECUTION_LOG_ERROR("LLVMEngine: Error reading current path '{}'", error.message());
      return;
    }
    llvm::sys::path::append(path, module.GetName(), ".to");
    auto file_buffer = llvm::MemoryBuffer::getFile(path);
    if (std::error_code error = file_buffer.getError()) {
      EXECUTION_LOG_ERROR("LLVMEngine: Error reading object file '{}'", error.message());
      return;
    }
    object_code_ = std::move(file_buffer.get());
  }

  EXECUTION_LOG_DEBUG("Object code size: {:.2f} KB", GetModuleObjectCodeSizeInBytes() / 1024.0);

  //
  // We've loaded the object file into an in-memory buffer. We need to convert
  // it into an object file, load it, and link it into our address space to make
  // its functions available for execution.
  //

  auto object = llvm::object::ObjectFile::createObjectFile(object_code_->getMemBufferRef());
  if (auto error = object.takeError()) {
    EXECUTION_LOG_ERROR("LLVMEngine: Error constructing object file '{}'", llvm::toString(std::move(error)));
    return;
  }

  llvm::RuntimeDyld loader(*memory_manager_, *memory_manager_);
  loader.loadObject(*object.get());
  if (loader.hasError()) {
    EXECUTION_LOG_ERROR("LLVMEngine: Error loading object file {}", loader.getErrorString().str());
    return;
  }
  loader.finalizeWithMemoryManagerLocking();

  //
  // Now, the object has successfully been loaded and is executable. We pull out
  // all module functions into a handy cache.
  //

  for (const auto &func : module.GetFunctionsInfo()) {
    auto symbol = loader.getSymbol(func.GetName());
    if (symbol.getAddress() == 0) {
      // for Mac portability
      symbol = loader.getSymbol("_" + func.GetName());
    }
    functions_[func.GetName()] = reinterpret_cast<void *>(symbol.getAddress());
    NOISEPAGE_ASSERT(symbol.getAddress() != 0, "symbol came out to be badly defined or missing");
  }

  // Done
  loaded_ = true;
}

// ---------------------------------------------------------
// LLVM Engine
// ---------------------------------------------------------

void LLVMEngine::Initialize() {
  // Global LLVM initialization
  llvm::InitializeNativeTarget();
  llvm::InitializeNativeTargetAsmPrinter();
  llvm::InitializeNativeTargetAsmParser();

  // Make all exported TPL symbols available to JITed code
  llvm::sys::DynamicLibrary::LoadLibraryPermanently(nullptr);
}

void LLVMEngine::Shutdown() { llvm::llvm_shutdown(); }

std::unique_ptr<LLVMEngine::CompiledModule> LLVMEngine::Compile(const BytecodeModule &module,
                                                                const CompilerOptions &options) {
  CompiledModuleBuilder builder(options, module);

  builder.DeclareStaticLocals();

  builder.DeclareFunctions();

  builder.DefineFunctions();

  builder.Simplify();

  builder.Verify();

  builder.Optimize();

  auto compiled_module = builder.Finalize();

  compiled_module->Load(module);

  return compiled_module;
}

}  // namespace noisepage::execution::vm
