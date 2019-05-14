#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "llvm/Support/MemoryBuffer.h"

#include "execution/util/common.h"
#include "execution/util/macros.h"
#include "execution/vm/bytecodes.h"

namespace tpl::ast {
class Type;
}  // namespace tpl::ast

namespace tpl::vm {

class BytecodeModule;
class FunctionInfo;
class LocalVar;

/// The interface to LLVM to JIT compile TPL bytecode
class LLVMEngine {
 public:
  // -------------------------------------------------------
  // Helper classes
  // -------------------------------------------------------

  class TPLMemoryManager;
  class TypeMap;
  class FunctionLocalsMap;
  class CompilerOptions;
  class CompiledModule;
  class CompiledModuleBuilder;

  // -------------------------------------------------------
  // Public API
  // -------------------------------------------------------

  /// Initialize the whole LLVM subsystem
  static void Initialize();

  /// Shutdown the whole LLVM subsystem
  static void Shutdown();

  /// JIT compile a TPL bytecode module to native code
  /// \param module The module to compile
  /// \return The JIT compiled module
  static std::unique_ptr<CompiledModule> Compile(
      const vm::BytecodeModule &module, const CompilerOptions &options = {});

  // -------------------------------------------------------
  // Compiler Options
  // -------------------------------------------------------

  /// Options to provide when compiling
  class CompilerOptions {
   public:
    CompilerOptions() : debug_(false), write_obj_file_(false) {}

    CompilerOptions &SetDebug(bool debug) {
      debug_ = debug;
      return *this;
    }

    bool IsDebug() const { return debug_; }

    CompilerOptions &SetPersistObjectFile(bool write_obj_file) {
      write_obj_file_ = write_obj_file;
      return *this;
    }

    bool ShouldPersistObjectFile() const { return write_obj_file_; }

    CompilerOptions &SetOutputObjectFileName(const std::string &name) {
      output_file_name_ = name;
      return *this;
    }

    const std::string &GetOutputObjectFileName() const {
      return output_file_name_;
    }

    std::string GetBytecodeHandlersBcPath() const {
      return "./bytecode_handlers_ir.bc";
    }

   private:
    bool debug_{false};
    bool write_obj_file_{false};
    std::string output_file_name_;
  };

  // -------------------------------------------------------
  // Compiled Module
  // -------------------------------------------------------

  /// A compiled module corresponds to a single TPL bytecode module that has
  /// been JIT compiled into native code.
  class CompiledModule {
   public:
    /// Constructor
    CompiledModule() : CompiledModule(nullptr) {}
    explicit CompiledModule(std::unique_ptr<llvm::MemoryBuffer> object_code);

    /// No copying or moving this class
    DISALLOW_COPY_AND_MOVE(CompiledModule);

    /// Destroy
    ~CompiledModule();

    /// Get a pointer to the jitted function in this module with name \a name
    /// \return A function pointer if a function exists with name \a name. If no
    ///         function exists, returns null.
    void *GetFunctionPointer(const std::string &name) const;

    /// Load the given module \a module into memory
    void Load(const BytecodeModule &module);

    /// Has this module been loaded into memory and linked?
    bool is_loaded() const { return loaded_; }

   private:
    bool loaded_;
    std::unique_ptr<llvm::MemoryBuffer> object_code_;
    std::unique_ptr<TPLMemoryManager> memory_manager_;
    std::unordered_map<std::string, void *> functions_;
  };
};

}  // namespace tpl::vm
