#pragma once

#include <memory>
#include <string>

#include "execution/ast/ast.h"
#include "execution/ast/context.h"
#include "execution/parsing/parser.h"
#include "execution/parsing/scanner.h"
#include "execution/sema/sema.h"
#include "execution/vm/bytecode_generator.h"
#include "execution/vm/bytecode_module.h"
#include "execution/vm/module.h"

namespace noisepage::execution::vm::test {

class ModuleCompiler {
 public:
  ModuleCompiler() : region_("temp"), errors_(&region_), ctx_(&region_, &errors_) {}

  ast::AstNode *CompileToAst(const std::string &source) {
    parsing::Scanner scanner(source);
    parsing::Parser parser(&scanner, &ctx_);

    auto *ast = parser.Parse();

    sema::Sema type_check(&ctx_);
    type_check.Run(ast);

    return ast;
  }

  std::unique_ptr<Module> CompileToModule(const std::string &source) {
    auto *ast = CompileToAst(source);
    if (HasErrors()) return nullptr;
    return std::make_unique<Module>(vm::BytecodeGenerator::Compile(ast, "test"));
  }

  // Does the error reporter have any errors?
  bool HasErrors() const { return errors_.HasErrors(); }

  void LogErrors() {
    if (errors_.HasErrors()) {
      EXECUTION_LOG_ERROR("CompileToAst error: {}", errors_.SerializeErrors());
    }
  }

  // Reset any previous errors
  void ClearErrors() { errors_.Reset(); }

 private:
  util::Region region_;
  sema::ErrorReporter errors_;
  ast::Context ctx_;
};

}  // namespace noisepage::execution::vm::test
