#include "execution/compiler/compiler.h"

#include <sstream>

#include "execution/ast/ast_pretty_print.h"
#include "execution/ast/context.h"
#include "execution/compiler/compiler_settings.h"
#include "execution/parsing/parser.h"
#include "execution/parsing/scanner.h"
#include "execution/sema/error_reporter.h"
#include "execution/sema/sema.h"
#include "execution/vm/bytecode_generator.h"
#include "execution/vm/module.h"
#include "execution/vm/module_metadata.h"

namespace noisepage::execution::compiler {

//===----------------------------------------------------------------------===//
//
// Compiler Input
//
//===----------------------------------------------------------------------===//

Compiler::Input::Input(std::string name, ast::Context *context, const std::string *source,
                       const CompilerSettings settings)
    : name_(std::move(name)), context_(context), root_(nullptr), source_(source), settings_(settings) {}

Compiler::Input::Input(std::string name, ast::Context *context, ast::AstNode *root, const CompilerSettings settings)
    : name_(std::move(name)), context_(context), root_(root), source_(nullptr), settings_(settings) {}

//===----------------------------------------------------------------------===//
//
// Compiler
//
//===----------------------------------------------------------------------===//

Compiler::Compiler(const Compiler::Input &input)
    : input_(input), root_(input.IsFromAST() ? input.root_ : nullptr), bytecode_module_(nullptr) {
  // Reset errors
  GetErrorReporter()->Reset();
}

// Required because we forward-declared the classes we use as templates to unique_ptr<> members.
Compiler::~Compiler() = default;

sema::ErrorReporter *Compiler::GetErrorReporter() const { return GetContext()->GetErrorReporter(); }

void Compiler::Run(Compiler::Callbacks *callbacks) {
  // -------------------------------------------------------
  // Phase 1 : Parsing
  // -------------------------------------------------------

  if (input_.IsFromSource()) {
    if (!callbacks->BeginPhase(Phase::Parsing, this)) {
      return;
    }

    parsing::Scanner scanner(*input_.source_);
    parsing::Parser parser(&scanner, GetContext());
    root_ = parser.Parse();

    callbacks->EndPhase(Phase::Parsing, this);
  }

  if (root_ == nullptr || GetErrorReporter()->HasErrors()) {
    callbacks->OnError(Phase::Parsing, this);
    return;
  }

  // -------------------------------------------------------
  // Phase 2 : Semantic Analysis (i.e., type-checking)
  // -------------------------------------------------------

  if (!callbacks->BeginPhase(Phase::SemanticAnalysis, this)) {
    return;
  }

  sema::Sema semantic_analysis(GetContext());
  semantic_analysis.Run(root_);

  if (GetErrorReporter()->HasErrors()) {
    callbacks->OnError(Phase::SemanticAnalysis, this);
    return;
  }

  callbacks->EndPhase(Phase::SemanticAnalysis, this);

  // -------------------------------------------------------
  // Phase 3 : Bytecode Generation
  // -------------------------------------------------------

  if (!callbacks->BeginPhase(Phase::BytecodeGeneration, this)) {
    return;
  }

  auto bytecode_module = vm::BytecodeGenerator::Compile(root_, input_.name_);
  bytecode_module_ = bytecode_module.get();

  if (GetErrorReporter()->HasErrors()) {
    callbacks->OnError(Phase::BytecodeGeneration, this);
    return;
  }

  callbacks->EndPhase(Phase::BytecodeGeneration, this);

  // -------------------------------------------------------
  // Phase 4 : Module Generation
  // -------------------------------------------------------

  if (!callbacks->BeginPhase(Phase::ModuleGeneration, this)) {
    return;
  }

  // Record any metadata necessary before the relevant data-structures are destroyed.
  const auto &settings = input_.GetSettings();
  vm::CompileTimeModuleMetadata metadata;
  {
    if (settings.ShouldCaptureTPL()) {
      std::ostringstream stream;
      ast::AstPrettyPrint::Dump(stream, root_);
      metadata.SetTPL(stream.str());
    }
    if (settings.ShouldCaptureTBC()) {
      std::ostringstream stream;
      bytecode_module->Dump(stream);
      metadata.SetTBC(stream.str());
    }
  }
  vm::ModuleMetadata module_metadata(std::move(metadata));

  auto module = std::make_unique<vm::Module>(std::move(bytecode_module), std::move(module_metadata));

  // Errors?
  if (GetErrorReporter()->HasErrors()) {
    callbacks->OnError(Phase::ModuleGeneration, this);
    return;
  }

  callbacks->EndPhase(Phase::ModuleGeneration, this);

  // -------------------------------------------------------
  // End
  // -------------------------------------------------------

  callbacks->TakeOwnership(std::move(module));
}

void Compiler::RunCompilation(const Compiler::Input &input, Compiler::Callbacks *callbacks) {
  NOISEPAGE_ASSERT(callbacks != nullptr, "Must provide callbacks");
  Compiler compiler(input);
  compiler.Run(callbacks);
}

namespace {

class NoOpCallbacks : public Compiler::Callbacks {
 public:
  NoOpCallbacks() : module_(nullptr) {}
  void OnError(Compiler::Phase phase, Compiler *compiler) override {}
  void TakeOwnership(std::unique_ptr<vm::Module> module) override { module_ = std::move(module); }
  std::unique_ptr<vm::Module> TakeModule() { return std::move(module_); }

 private:
  std::unique_ptr<vm::Module> module_;
};

}  // namespace

std::unique_ptr<vm::Module> Compiler::RunCompilationSimple(const Compiler::Input &input) {
  NoOpCallbacks no_op_callbacks;
  RunCompilation(input, &no_op_callbacks);
  return no_op_callbacks.TakeModule();
}

std::string Compiler::CompilerPhaseToString(Compiler::Phase phase) {
  switch (phase) {
    case Phase::Parsing:
      return "Parsing";
    case Phase::SemanticAnalysis:
      return "Semantic Analysis";
    case Phase::BytecodeGeneration:
      return "Bytecode Generation";
    case Phase::ModuleGeneration:
      return "Module Generation";
    default:
      UNREACHABLE("Impossible.");
  }
}

}  // namespace noisepage::execution::compiler
