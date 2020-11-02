#pragma once

#include <llvm/Support/MemoryBuffer.h>

#include <memory>
#include <string>
#include <unordered_map>

#include "common/macros.h"
#include "execution/util/execution_common.h"

namespace noisepage::execution::ast {
class Type;
}  // namespace noisepage::execution::ast

namespace noisepage::execution::vm {

class BytecodeModule;
class FunctionInfo;
class LocalVar;

/**
 * The interface to LLVM to JIT compile TPL bytecode
 */
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

  /**
   * Initialize the whole LLVM subsystem
   */
  static void Initialize();

  /**
   * Shutdown the whole LLVM subsystem
   */
  static void Shutdown();

  /**
   * JIT compile a TPL bytecode module to native code
   * @param module The module to compile
   * @param options The compiler options
   * @return The JIT compiled module
   */
  static std::unique_ptr<CompiledModule> Compile(const BytecodeModule &module, const CompilerOptions &options);

  // -------------------------------------------------------
  // Compiler Options
  // -------------------------------------------------------

  /**
   * Options to provide when compiling
   */
  class CompilerOptions {
   public:
    /**
     * Constructor. Turns off all options
     */
    CompilerOptions() = default;

    /**
     * Set the debug option
     * @param debug debug options
     * @return the updated object
     */
    CompilerOptions &SetDebug(bool debug) {
      debug_ = debug;
      (void)debug_;
      return *this;
    }

    /**
     * @return whether debugging is on
     */
    bool IsDebug() const { return debug_; }

    /**
     * Set the persisting option
     * @param write_obj_file the persisting options
     * @return the updated object
     */
    CompilerOptions &SetPersistObjectFile(bool write_obj_file) {
      write_obj_file_ = write_obj_file;
      return *this;
    }

    /**
     * @return whether the persisting flag is on
     */
    bool ShouldPersistObjectFile() const { return write_obj_file_; }

    /**
     * Set the output file name
     * @param name name of the file
     * @return the updated object
     */
    CompilerOptions &SetOutputObjectFileName(const std::string &name) {
      output_file_name_ = name;
      return *this;
    }

    /**
     * @return the output file name
     */
    const std::string &GetOutputObjectFileName() const { return output_file_name_; }

    /**
     * @return the path to the bytecode handlers bitcode file.
     */
    std::string GetBytecodeHandlersBcPath() const { return "./bytecode_handlers_ir.bc"; }

   private:
    bool debug_{false};
    bool write_obj_file_{false};
    std::string output_file_name_;
  };

  // -------------------------------------------------------
  // Compiled Module
  // -------------------------------------------------------

  /**
   * A compiled module corresponds to a single TPL bytecode module that has
   * been JIT compiled into native code.
   */
  class CompiledModule {
   public:
    /**
     * Construct a module without an in-memory module. Users must call @em
     * Load() to load in a pre-compiled shared object library for this compiled
     * module before this module's functions can be invoked.
     */
    CompiledModule() : CompiledModule(nullptr) {}

    /**
     * Construct a compiled module using the provided shared object file.
     * @param object_code The object file containing code for this module.
     */
    explicit CompiledModule(std::unique_ptr<llvm::MemoryBuffer> object_code);

    /**
     * This class cannot be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(CompiledModule);

    /**
     * Destroy
     */
    ~CompiledModule();

    /**
     * Get a pointer to the JIT-ed function in this module with name @em name.
     * @return A function pointer if a function with the provided name exists.
     *         If no such function exists, returns null.
     */
    void *GetFunctionPointer(const std::string &name) const;

    /**
     * Return the size of the module's object code in-memory in bytes.
     */
    std::size_t GetModuleObjectCodeSizeInBytes() const { return object_code_->getBufferSize(); }

    /**
     * Load the given module @em module into memory. If this module has already
     * been loaded, it will not be reloaded.
     */
    void Load(const BytecodeModule &module);

    /**
     * Has this module been loaded into memory and linked?
     */
    bool IsLoaded() const { return loaded_; }

   private:
    bool loaded_;
    std::unique_ptr<llvm::MemoryBuffer> object_code_;
    std::unique_ptr<TPLMemoryManager> memory_manager_;
    std::unordered_map<std::string, void *> functions_;
  };
};

}  // namespace noisepage::execution::vm
