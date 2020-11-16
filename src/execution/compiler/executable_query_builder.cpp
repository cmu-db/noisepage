#include "execution/compiler/executable_query_builder.h"

#include <iostream>

#include "execution/ast/ast_dump.h"
#include "execution/ast/ast_node_factory.h"
#include "execution/ast/ast_pretty_print.h"
#include "execution/ast/context.h"
#include "execution/compiler/compiler.h"
#include "execution/sema/error_reporter.h"
#include "execution/vm/module.h"
#include "loggers/execution_logger.h"

namespace noisepage::execution::compiler {

ExecutableQueryFragmentBuilder::ExecutableQueryFragmentBuilder(ast::Context *ctx) : ctx_(ctx) {}

void ExecutableQueryFragmentBuilder::RegisterStep(ast::FunctionDecl *decl) {
  functions_.push_back(decl);
  step_functions_.push_back(decl->Name().GetString());
}

namespace {

class Callbacks : public compiler::Compiler::Callbacks {
 public:
  Callbacks() : module_(nullptr) {}

  void OnError(compiler::Compiler::Phase phase, compiler::Compiler *compiler) override {
    // TODO(WAN): how should we report errors? Probably refactor pretty print dump to serialize to string.
    EXECUTION_LOG_ERROR(fmt::format("ERROR: {}", compiler->GetErrorReporter()->SerializeErrors()));
    ast::AstPrettyPrint::Dump(std::cerr, compiler->GetAST());  // NOLINT
  }

  void TakeOwnership(std::unique_ptr<vm::Module> module) override { module_ = std::move(module); }

  std::unique_ptr<vm::Module> ReleaseModule() { return std::move(module_); }

 private:
  std::unique_ptr<vm::Module> module_;
};

}  // namespace

std::unique_ptr<ExecutableQuery::Fragment> ExecutableQueryFragmentBuilder::Compile() {
  // Build up the declaration list for the file.
  util::RegionVector<ast::Decl *> decls(ctx_->GetRegion());
  decls.reserve(structs_.size() + functions_.size());
  decls.insert(decls.end(), structs_.begin(), structs_.end());
  decls.insert(decls.end(), functions_.begin(), functions_.end());

  // The file we'll compile.
  ast::File *generated_file = ctx_->GetNodeFactory()->NewFile({0, 0}, std::move(decls));

  // Compile it!
  compiler::Compiler::Input input("", ctx_, generated_file);
  Callbacks callbacks;
  compiler::TimePasses timer(&callbacks);
  compiler::Compiler::RunCompilation(input, &timer);
  std::unique_ptr<vm::Module> module = callbacks.ReleaseModule();

  EXECUTION_LOG_DEBUG("Type-check: {:.2f} ms, Bytecode Gen: {:.2f} ms, Module Gen: {:.2f} ms", timer.GetSemaTimeMs(),
                      timer.GetBytecodeGenTimeMs(), timer.GetModuleGenTimeMs());

  // Create the fragment.
  std::vector<std::string> teardown_names;
  for (auto &decl : teardown_fn_) {
    teardown_names.push_back(decl->Name().GetString());
  }
  return std::make_unique<ExecutableQuery::Fragment>(std::move(step_functions_), std::move(teardown_names),
                                                     std::move(module));
}

}  // namespace noisepage::execution::compiler
