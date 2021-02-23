#pragma once

#include <llvm/Support/MemoryBuffer.h>

#include <memory>
#include <string>
#include <string_view>
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
  class Settings;

  // -------------------------------------------------------
  // Public API
  // -------------------------------------------------------

  /**
   * Initialize the whole LLVM subsytem and LLVM engine settings
   * @param settings The settings with which the engine should be initialized
   */
  static void Initialize(std::unique_ptr<const Settings> &&settings);

  /**
   * Shutdown the whole LLVM subsystem.
   */
  static void Shutdown();

  /**
   * JIT compile a TPL bytecode module to native code
   * @param module The module to compile
   * @param options The compiler options
   * @return The JIT compiled module
   */
  static std::unique_ptr<CompiledModule> Compile(const BytecodeModule &module, const CompilerOptions &options);

  /**
   * @return A non-mutating pointer to the LLVM engine settings
   */
  static const Settings *GetEngineSettings();

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

  // -------------------------------------------------------
  // LLVM Engine Settings
  // -------------------------------------------------------

  /**
   * Engine-wide settings that apply for the entire process in which the LLVM Engine runs.
   */
  class Settings {
   public:
    /**
     * Construct a settings instance from the relevant configuration parameters.
     * @param bytecode_handlers_path The path to the bytecode handlers bitcode file.
     */
    explicit Settings(std::string_view bytecode_handlers_path) : bytecode_handlers_path_{bytecode_handlers_path} {}

    /**
     * @return The path to the bytecode handlers bitcode file.
     */
    const std::string &GetBytecodeHandlersBcPath() const noexcept { return bytecode_handlers_path_; }

   private:
    const std::string bytecode_handlers_path_;
  };

  // -------------------------------------------------------
  // Static Data Members
  // -------------------------------------------------------

  /**
   * Process-wide LLVM engine settings.
   *
   * TODO(Kyle): I'm not particularly happy with this setup - an inline
   * static variable (essentially just a global with scoping) for managing
   * the settings for the LLVM engine. The ownership model should be
   * relatively simple - the LLVMEngine should own its settings, but
   * obviously the issue is, currently, LLVMEngine is a totally static
   * class. This is a nice property, and makes using it in other locations
   * within the execution engine easy and clean e.g. LLVMEngine::Compile(...).
   * This leaves us with a couple of options for managing this state that
   * is "global" to the engine for the entire process, I've tried:
   * - A weird, mutant PImpl implementation that was essentially just a
   *   more convoluted version of the current implementation
   * - A singleton - this is the obvious "first attempt" at solving this
   *   problem, and it works, but its kind of gross and no one really likes
   *   singletons
   * - Removing the state from the LLVMEngine entirely and instead
   *   "pushing it down" into the module (and fragments) during compilation,
   *   but this was more complex, required many API changes, and ultimately
   *   got away from the point above - that this state should be owned by
   *   the LLVMEngine because that is the natural ownership relationship
   */
  inline static std::unique_ptr<const Settings> engine_settings;  // NOLINT
};

}  // namespace noisepage::execution::vm
