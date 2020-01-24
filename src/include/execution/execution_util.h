#pragma once

#include <memory>

#include "execution/ast/ast_dump.h"
#include "execution/ast/context.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compiler.h"
#include "execution/parsing/parser.h"
#include "execution/parsing/scanner.h"
#include "execution/sema/sema.h"
#include "execution/table_generator/sample_output.h"
#include "execution/util/cpu_info.h"
#include "execution/util/region.h"
#include "execution/vm/bytecode_generator.h"
#include "execution/vm/llvm_engine.h"
#include "execution/vm/module.h"
#include "loggers/execution_logger.h"

namespace terrier::execution {

class ExecutableQuery {
 public:
  ExecutableQuery(const common::ManagedPointer<planner::AbstractPlanNode> physical_plan,
                  const common::ManagedPointer<exec::ExecutionContext> exec_ctx) {
    // Compile and check for errors
    compiler::CodeGen codegen(exec_ctx.Get());
    compiler::Compiler compiler(&codegen, physical_plan.Get());
    auto root = compiler.Compile();
    if (codegen.Reporter()->HasErrors()) {
      EXECUTION_LOG_ERROR("Type-checking error! \n {}", codegen.Reporter()->SerializeErrors());
    }

    EXECUTION_LOG_DEBUG("Converted: \n {}", execution::ast::AstDump::Dump(root));

    // Convert to bytecode
    auto bytecode_module = vm::BytecodeGenerator::Compile(root, exec_ctx.Get(), "tmp-tpl");

    tpl_module_ = std::make_unique<vm::Module>(std::move(bytecode_module));
    region_ = codegen.ReleaseRegion();
    ast_ctx_ = codegen.ReleaseContext();
  }

  /**
   * Construct and compile an executable TPL program in the given filename
   *
   * @param filename The name of the file on disk to compile
   * @param exec_ctx context to execute
   */
  ExecutableQuery(const std::string &filename, const common::ManagedPointer<exec::ExecutionContext> exec_ctx) {
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
    sample_output_ = std::make_unique<exec::SampleOutput>();
    sample_output_->InitTestOutput();
    auto output_schema = sample_output_->GetSchema(query_name_);
    printer_ = std::make_unique<exec::OutputPrinter>(output_schema);
  }

  void Run(const common::ManagedPointer<exec::ExecutionContext> exec_ctx, const vm::ExecutionMode mode) {
    auto params = GetQueryParams();
    exec_ctx->SetParams(std::move(params));

    // Run the main function
    std::function<int64_t(exec::ExecutionContext *)> main;
    if (!tpl_module_->GetFunction("main", mode, &main)) {
      EXECUTION_LOG_ERROR(
          "Missing 'main' entry function with signature "
          "(*ExecutionContext)->int32");
      return;
    }
    auto result = main(exec_ctx.Get());
    EXECUTION_LOG_INFO("main() returned: {}", result);
  }

  const planner::OutputSchema *GetOutputSchema() const { return sample_output_->GetSchema(query_name_);; }
  const exec::OutputPrinter &GetPrinter() const { return *printer_; }

 private:
  static std::string GetFileName(const std::string &path) {
    std::size_t size = path.size();
    std::size_t found = path.find_last_of("/\\");
    return path.substr(found + 1, size - found - 5);
  }

  std::vector<type::TransientValue> GetQueryParams() {
    std::vector<type::TransientValue> params;
    if (query_name_ == "tpch_q5")
      params.emplace_back(type::TransientValueFactory::GetVarChar("ASIA"));

    // Add the identifier for each pipeline. At most 8 query pipelines for now
    for (int i = 0; i < 8; ++i)
      params.emplace_back(type::TransientValueFactory::GetVarChar(query_name_ + "_p" + std::to_string(i + 1)));

    return params;
  }

  // TPL bytecodes for this query
  std::unique_ptr<vm::Module> tpl_module_;

  // Memory region and AST context from the code generation stage that need to stay alive as long as the TPL module will
  // be executed. Direct access to these objects is likely unneeded from this class, we just want to tie the life cycles
  // together.
  std::unique_ptr<util::Region> region_;
  std::unique_ptr<ast::Context> ast_ctx_;

  // Used to specify the output for this query
  std::unique_ptr<exec::SampleOutput> sample_output_;
  std::unique_ptr<exec::OutputPrinter> printer_;
  std::string query_name_;
};

class ExecutionUtil {
 public:
  ExecutionUtil() = delete;

  /**
   * Initialize all TPL subsystems
   */
  static void InitTPL() {
    execution::CpuInfo::Instance();
    execution::vm::LLVMEngine::Initialize();
  }

  /**
   * Shutdown all TPL subsystems
   */
  static void ShutdownTPL() { terrier::execution::vm::LLVMEngine::Shutdown(); }
};

}  // namespace terrier::execution 