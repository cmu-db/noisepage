#pragma once

#include <array>
#include <memory>
#include <string>
#include <utility>

#include "common/macros.h"
#include "common/managed_pointer.h"
#include "execution/util/timer.h"

namespace noisepage::execution {

namespace ast {
class AstNode;
class Context;
}  // namespace ast

namespace exec {
class ExecutionSettings;
}  // namespace exec

namespace sema {
class ErrorReporter;
}  // namespace sema

namespace vm {
class BytecodeModule;
class Module;
}  // namespace vm

namespace compiler {

/**
 * Primary interface to drive compilation of TPL programs into TPL modules. TPL modules support
 * hybrid interpreted and JIT execution modes.
 */
class Compiler {
 public:
  /**
   * Phases of compilation.
   */
  enum class Phase : uint8_t {
    Parsing,
    SemanticAnalysis,
    BytecodeGeneration,
    ModuleGeneration,

    // Never add elements below this comment
    Last
  };

  /**
   * Input into a compilation job.
   */
  class Input {
    friend class Compiler;

   public:
    /**
     * Construct an input from a TPL source program.
     * @param name The name to assign the input.
     * @param context The TPL context to use.
     * @param source The TPL source code.
     */
    Input(std::string name, ast::Context *context, const std::string *source);

    /**
     * Construct input from a pre-generated TPL AST. The region that created the AST must also be
     * moved into this input, which will take ownership of it.
     * @param name The name to assign the input.
     * @param context The TPL context the AST belongs to.
     * @param root The root of the AST.
     */
    Input(std::string name, ast::Context *context, ast::AstNode *root);

    /**
     * @return The name of the input.
     */
    const std::string &GetName() const noexcept { return name_; }

    /**
     * @return True if the input is raw TPL source code; false otherwise.
     */
    bool IsFromSource() const noexcept { return source_ != nullptr; }

    /**
     * @return True if the input is a pre-generated TPL AST; false otherwise.
     */
    bool IsFromAST() const noexcept { return root_ != nullptr; }

    /**
     * @return The TPL context.
     */
    ast::Context *GetContext() const noexcept { return context_; }

   private:
    // The name to assign the input
    const std::string name_;
    // The context that created the AST
    ast::Context *context_;
    // The root of the AST, if any
    ast::AstNode *root_;
    // The TPL source, if any
    const std::string *source_;
  };

  /**
   * Callback interface used to notify users of the various stages of compilation.
   */
  class Callbacks {
   public:
    /**
     * Virtual destructor.
     */
    virtual ~Callbacks() = default;

    /**
     * Called before starting the provided phase of compilation.
     * @param phase The phase of compilation that will begin.
     * @param compiler The compiler instance.
     * @return True if compilation should continue; false otherwise.
     */
    virtual bool BeginPhase(Phase phase, Compiler *compiler) { return true; }

    /**
     * Invoked after the provided phase of compilation.
     * @param phase The phase that has ended.
     * @param compiler The compiler instance.
     */
    virtual void EndPhase(Phase phase, Compiler *compiler) {}

    /**
     * Invoked when an error occurs during compilation. Compilation will NOT continue after this
     * method call.
     * @param phase The phase of compilation where the error occurred.
     * @param compiler The compiler instance.
     */
    virtual void OnError(Phase phase, Compiler *compiler) = 0;

    /**
     * Arrange for the caller to take ownership of the generated module produced by the compiler.
     * @param module The generated module.
     */
    virtual void TakeOwnership(std::unique_ptr<vm::Module> module) = 0;
  };

  /** Tracks the number of phases */
  static constexpr uint32_t NUM_PHASES = static_cast<uint32_t>(Phase::Last);

  /**
   * Main entry point into the TPL compilation pipeline. Accepts an input program (in the form of
   * raw TPL source or a pre-generated AST), passes it through all required compilation passes and
   * produces a TPL module. The TPL module is provided to the callback.
   * @param input The input into the compiler.
   * @param callbacks The callbacks.
   */
  static void RunCompilation(const Input &input, Callbacks *callbacks);

  /**
   * Compile the given input into a module.
   * @param input The input to compilation.
   * @return The generated module. If there was an error, returns NULL.
   */
  static std::unique_ptr<vm::Module> RunCompilationSimple(const Input &input);

  /**
   * @return A string name for the given compilation phase.
   */
  static std::string CompilerPhaseToString(Phase phase);

  /**
   * @return The TPL context used during compilation.
   */
  ast::Context *GetContext() const { return input_.GetContext(); }

  /**
   * @return The diagnostic error reporter used during compilation.
   */
  sema::ErrorReporter *GetErrorReporter() const;

  /**
   * @return The AST for the compiled program. If parsing is not complete, or if there was an error
   *         during compilation, will be null.
   */
  ast::AstNode *GetAST() const { return root_; }

  /**
   * @return The generated bytecode module. If code-generation has not begun, or if there was an
   *         error during compilation, will be null.
   */
  vm::BytecodeModule *GetBytecodeModule() const { return bytecode_module_; }

 private:
  // Create a compiler instance
  explicit Compiler(const Compiler::Input &input);

  // This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(Compiler);

  // Destructor
  ~Compiler();

  // Driver
  void Run(Compiler::Callbacks *callbacks);

 private:
  // The input to compilation
  const Compiler::Input &input_;
  // The parsed AST
  ast::AstNode *root_;
  // The generated bytecode module
  vm::BytecodeModule *bytecode_module_;
};

/**
 * Utility class to time phases of compilation.
 */
class TimePasses : public Compiler::Callbacks {
 public:
  /**
   * Create a new timer instance.
   * @param wrapped_callbacks The callbacks to be wrapped that are being timed.
   */
  explicit TimePasses(Compiler::Callbacks *wrapped_callbacks)
      : wrapped_callbacks_(wrapped_callbacks), phase_timings_{0.0} {}

  bool BeginPhase(Compiler::Phase phase, Compiler *compiler) override {
    timer_.Start();
    return wrapped_callbacks_->BeginPhase(phase, compiler);
  }

  void EndPhase(Compiler::Phase phase, Compiler *compiler) override {
    timer_.Stop();
    phase_timings_[static_cast<uint32_t>(phase)] = timer_.GetElapsed();
    wrapped_callbacks_->EndPhase(phase, compiler);
  }

  void OnError(Compiler::Phase phase, Compiler *compiler) override { wrapped_callbacks_->OnError(phase, compiler); }

  void TakeOwnership(std::unique_ptr<vm::Module> module) override {
    wrapped_callbacks_->TakeOwnership(std::move(module));
  }

  /** @return Time taken for parsing. */
  double GetParseTimeMs() const { return phase_timings_[static_cast<uint32_t>(Compiler::Phase::Parsing)]; }

  /** @return Time taken for semantic analysis. */
  double GetSemaTimeMs() const { return phase_timings_[static_cast<uint32_t>(Compiler::Phase::SemanticAnalysis)]; }

  /** @return Time taken for bytecode generation. */
  double GetBytecodeGenTimeMs() const {
    return phase_timings_[static_cast<uint32_t>(Compiler::Phase::BytecodeGeneration)];
  }

  /** @return Time taken for module generation. */
  double GetModuleGenTimeMs() const { return phase_timings_[static_cast<uint32_t>(Compiler::Phase::ModuleGeneration)]; }

 private:
  // Wrapped callbacks
  Compiler::Callbacks *wrapped_callbacks_;
  // The timer we use to time phases
  util::Timer<std::milli> timer_;
  // Timings of each phase
  std::array<double, Compiler::NUM_PHASES> phase_timings_;
};

}  // namespace compiler
}  // namespace noisepage::execution
