#include "execution/executable_query.h"

#include <llvm/ADT/StringRef.h>
#include <llvm/Support/ErrorOr.h>
#include <llvm/Support/MemoryBuffer.h>

#include <cstdint>
#include <stdexcept>
#include <string>
#include <system_error>  // NOLINT
#include <type_traits>

#include "common/macros.h"
#include "execution/ast/ast.h"
#include "execution/ast/ast_dump.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compiler.h"
#include "execution/exec/execution_context.h"
#include "execution/parsing/parser.h"
#include "execution/parsing/scanner.h"
#include "execution/sema/error_reporter.h"
#include "execution/sema/sema.h"
#include "execution/util/region.h"
#include "execution/vm/bytecode_generator.h"
#include "execution/vm/module.h"
#include "loggers/execution_logger.h"

namespace terrier::execution {

std::atomic<query_id_t> ExecutableQuery::query_identifier{query_id_t{0}};

ExecutableQuery::ExecutableQuery(const common::ManagedPointer<planner::AbstractPlanNode> physical_plan,
                                 const common::ManagedPointer<exec::ExecutionContext> exec_ctx) {
  // Generate a query id using std::atomic<>.fetch_add()
  query_id_ = ExecutableQuery::query_identifier++;

  // Compile and check for errors
  compiler::CodeGen codegen(exec_ctx.Get());
  compiler::Compiler compiler(query_id_, &codegen, physical_plan.Get());
  auto root = compiler.Compile();
  if (codegen.Reporter()->HasErrors()) {
    EXECUTION_LOG_ERROR("Type-checking error! \n {}", codegen.Reporter()->SerializeErrors());
    EXECUTION_LOG_ERROR("Dumping AST:");
    EXECUTION_LOG_ERROR(execution::ast::AstDump::Dump(root));
    return;
  }

  // Convert to bytecode
  auto bytecode_module = vm::BytecodeGenerator::Compile(root, exec_ctx.Get(), "tmp-tpl");

  tpl_module_ = std::make_unique<vm::Module>(std::move(bytecode_module));
  region_ = codegen.ReleaseRegion();
  ast_ctx_ = codegen.ReleaseContext();
  pipeline_operating_units_ = codegen.ReleasePipelineOperatingUnits();
  exec_ctx->SetPipelineOperatingUnits(common::ManagedPointer(pipeline_operating_units_));
}

ExecutableQuery::ExecutableQuery(const std::string &filename,
                                 const common::ManagedPointer<exec::ExecutionContext> exec_ctx) {
  auto file = llvm::MemoryBuffer::getFile(filename);
  if (std::error_code error = file.getError()) {
    EXECUTION_LOG_ERROR("There was an error reading file '{}': {}", filename, error.message());
    return;
  }

  // Copy the source into a temporary, compile, and run
  auto source = (*file)->getBuffer().str();

  // Let's scan the source
  region_ = std::make_unique<util::Region>("repl-ast");
  util::Region error_region("repl-error");
  sema::ErrorReporter error_reporter(&error_region);
  ast_ctx_ = std::make_unique<ast::Context>(region_.get(), &error_reporter);

  parsing::Scanner scanner(source.data(), source.length());
  parsing::Parser parser(&scanner, ast_ctx_.get());

  // Parse
  ast::AstNode *root = parser.Parse();
  if (error_reporter.HasErrors()) {
    EXECUTION_LOG_ERROR("Parsing errors: \n {}", error_reporter.SerializeErrors());
    throw std::runtime_error("Parsing Error!");
  }

  // Type check
  sema::Sema type_check(ast_ctx_.get());
  type_check.Run(root);
  if (error_reporter.HasErrors()) {
    EXECUTION_LOG_ERROR("Type-checking errors: \n {}", error_reporter.SerializeErrors());
    throw std::runtime_error("Type Checking Error!");
  }

  EXECUTION_LOG_DEBUG("Converted: \n {}", execution::ast::AstDump::Dump(root));

  // Convert to bytecode
  auto bytecode_module = vm::BytecodeGenerator::Compile(root, exec_ctx.Get(), "tmp-tpl");
  tpl_module_ = std::make_unique<vm::Module>(std::move(bytecode_module));

  // acquire the output format
  query_name_ = GetFileName(filename);
}

void ExecutableQuery::Run(const common::ManagedPointer<exec::ExecutionContext> exec_ctx, const vm::ExecutionMode mode) {
  TERRIER_ASSERT(tpl_module_ != nullptr, "Trying to run a module that failed to compile.");
  exec_ctx->SetExecutionMode(static_cast<uint8_t>(mode));

  // Run the main function
  if (!tpl_module_->GetFunction("main", mode, &main_)) {
    EXECUTION_LOG_ERROR(
        "Missing 'main' entry function with signature "
        "(*ExecutionContext)->int32");
  }
  auto result = main_(exec_ctx.Get());
  EXECUTION_LOG_DEBUG("main() returned: {}", result);
  exec_ctx->SetPipelineOperatingUnits(nullptr);
}

}  // namespace terrier::execution
