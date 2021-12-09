#include "execution/compiler/executable_query.h"

#include <algorithm>

#include "common/error/error_code.h"
#include "common/error/exception.h"
#include "execution/ast/ast.h"
#include "execution/ast/ast_dump.h"
#include "execution/ast/context.h"
#include "execution/compiler/compiler.h"
#include "execution/exec/execution_context.h"
#include "execution/sema/error_reporter.h"
#include "execution/vm/bytecode_function_info.h"
#include "execution/vm/module.h"
#include "loggers/execution_logger.h"
#include "self_driving/modeling/operating_unit.h"
#include "transaction/transaction_context.h"

namespace noisepage::execution::compiler {

//===----------------------------------------------------------------------===//
//
// Executable Query Fragment
//
//===----------------------------------------------------------------------===//

ExecutableQuery::Fragment::Fragment(std::vector<std::string> &&functions, std::vector<std::string> &&teardown_fns,
                                    std::unique_ptr<vm::Module> module)
    : functions_{std::move(functions)}, teardown_fns_{std::move(teardown_fns)}, module_{std::move(module)} {}

ExecutableQuery::Fragment::Fragment(std::vector<std::string> &&functions, std::vector<std::string> &&teardown_fns,
                                    std::unique_ptr<vm::Module> module, ast::File *file)
    : functions_{std::move(functions)},
      teardown_fns_{std::move(teardown_fns)},
      module_{std::move(module)},
      file_{file} {}

ExecutableQuery::Fragment::~Fragment() = default;

void ExecutableQuery::Fragment::Run(byte query_state[], vm::ExecutionMode mode) const {
  using Function = std::function<void(void *)>;

  auto exec_ctx = *reinterpret_cast<exec::ExecutionContext **>(query_state);
  if (exec_ctx->GetTxn()->MustAbort()) {
    return;
  }
  for (const auto &func_name : functions_) {
    Function func;
    if (!module_->GetFunction(func_name, mode, &func)) {
      throw EXECUTION_EXCEPTION(fmt::format("Could not find function '{}' in query fragment.", func_name),
                                common::ErrorCode::ERRCODE_INTERNAL_ERROR);
    }
    try {
      func(query_state);
    } catch (const AbortException &e) {
      for (const auto &teardown_name : teardown_fns_) {
        if (!module_->GetFunction(teardown_name, mode, &func)) {
          throw EXECUTION_EXCEPTION(fmt::format("Could not find teardown function '{}' in query fragment.", func_name),
                                    common::ErrorCode::ERRCODE_INTERNAL_ERROR);
        }
        func(query_state);
      }
      return;
    }
  }
}

std::optional<const vm::FunctionInfo *> ExecutableQuery::Fragment::GetFunctionMetadata(const std::string &name) const {
  const auto *metadata = module_->GetFuncInfoByName(name);
  return (metadata == nullptr) ? std::nullopt : std::make_optional(metadata);
}

const vm::ModuleMetadata &ExecutableQuery::Fragment::GetModuleMetadata() const { return module_->GetMetadata(); }

//===----------------------------------------------------------------------===//
//
// Executable Query
//
//===----------------------------------------------------------------------===//

// For mini_runners.cpp.
namespace {
std::string GetFileName(const std::string &path) {
  std::size_t size = path.size();
  std::size_t found = path.find_last_of("/\\");
  return path.substr(found + 1, size - found - 5);
}
}  // namespace

// We use 0 to represent NULL_QUERY_ID so the query id starts from 1.
std::atomic<query_id_t> ExecutableQuery::query_identifier{1};

void ExecutableQuery::SetPipelineOperatingUnits(std::unique_ptr<selfdriving::PipelineOperatingUnits> &&units) {
  pipeline_operating_units_ = std::move(units);
}

ExecutableQuery::ExecutableQuery(const planner::AbstractPlanNode &plan, const exec::ExecutionSettings &exec_settings,
                                 transaction::timestamp_t timestamp, ast::Context *context)
    : plan_{plan},
      exec_settings_{exec_settings},
      timestamp_{timestamp},
      context_region_{std::make_unique<util::Region>("context_region")},
      errors_region_{std::make_unique<util::Region>("errors_region")},
      errors_{std::make_unique<sema::ErrorReporter>(errors_region_.get())},
      ast_context_{context},
      query_state_size_{0},
      pipeline_operating_units_{nullptr},
      query_id_{query_identifier++} {
  owns_ast_context_ = (ast_context_ == nullptr);
  if (owns_ast_context_) {
    ast_context_ = new ast::Context(context_region_.get(), errors_.get());
  }
}

ExecutableQuery::ExecutableQuery(const std::string &contents,
                                 const common::ManagedPointer<exec::ExecutionContext> exec_ctx, bool is_file,
                                 std::size_t query_state_size, const exec::ExecutionSettings &exec_settings,
                                 transaction::timestamp_t timestamp, ast::Context *context)
    // TODO(WAN): Giant hack for the plan. The whole point is that you have no plan.
    : plan_{reinterpret_cast<const planner::AbstractPlanNode &>(exec_settings)},
      exec_settings_{exec_settings},
      timestamp_{timestamp},
      context_region_{std::make_unique<util::Region>("context_region")},
      errors_region_{std::make_unique<util::Region>("error_region")},
      errors_{std::make_unique<sema::ErrorReporter>(errors_region_.get())},
      ast_context_{context},
      query_state_size_{0},
      pipeline_operating_units_{nullptr},
      query_id_{query_identifier++} {
  owns_ast_context_ = (ast_context_ == nullptr);
  if (owns_ast_context_) {
    ast_context_ = new ast::Context(context_region_.get(), errors_.get());
  }

  // Let's scan the source
  std::string source{};
  if (is_file) {
    auto file = llvm::MemoryBuffer::getFile(contents);
    if (std::error_code error = file.getError()) {
      EXECUTION_LOG_ERROR("There was an error reading file '{}': {}", contents, error.message());
      return;
    }
    // Copy the source into a temporary, compile, and run
    source = (*file)->getBuffer().str();
  } else {
    source = contents;
  }

  auto input = Compiler::Input("tpl_source", ast_context_, &source, exec_settings.GetCompilerSettings());
  auto module = compiler::Compiler::RunCompilationSimple(input);

  std::vector<std::string> functions{"main"};
  std::vector<std::string> teardown_functions{};

  auto fragment = std::make_unique<Fragment>(std::move(functions), std::move(teardown_functions), std::move(module));

  std::vector<std::unique_ptr<Fragment>> fragments{};
  fragments.emplace_back(std::move(fragment));

  Setup(std::move(fragments), query_state_size, nullptr);

  if (is_file) {
    // acquire the output format
    query_name_ = GetFileName(contents);
  }
}

// Needed because we forward-declare classes used as template types to std::unique_ptr<>
ExecutableQuery::~ExecutableQuery() {
  if (owns_ast_context_) {
    delete ast_context_;
  }
}
void ExecutableQuery::Setup(std::vector<std::unique_ptr<Fragment>> &&fragments, const std::size_t query_state_size,
                            std::unique_ptr<selfdriving::PipelineOperatingUnits> pipeline_operating_units) {
  NOISEPAGE_ASSERT(
      std::all_of(fragments.begin(), fragments.end(), [](const auto &fragment) { return fragment->IsCompiled(); }),
      "All query fragments are not compiled!");
  NOISEPAGE_ASSERT(query_state_size >= sizeof(void *),
                   "Query state must be large enough to store at least an ExecutionContext pointer.");

  fragments_ = std::move(fragments);
  query_state_size_ = query_state_size;
  pipeline_operating_units_ = std::move(pipeline_operating_units);

  EXECUTION_LOG_TRACE("Query has {} fragment{} with {}-byte query state.", fragments_.size(),
                      fragments_.size() > 1 ? "s" : "", query_state_size_);
}

void ExecutableQuery::Run(common::ManagedPointer<exec::ExecutionContext> exec_ctx, vm::ExecutionMode mode) {
  // First, allocate the query state and move the execution context into it.
  auto query_state = std::make_unique<byte[]>(query_state_size_);
  *reinterpret_cast<exec::ExecutionContext **>(query_state.get()) = exec_ctx.Get();

  exec_ctx->SetExecutionMode(mode);
  exec_ctx->SetQueryState(query_state.get());
  exec_ctx->SetPipelineOperatingUnits(GetPipelineOperatingUnits());
  exec_ctx->SetQueryId(query_id_);

  // Now run through fragments.
  for (const auto &fragment : fragments_) {
    fragment->Run(query_state.get(), mode);
  }
}

std::vector<std::string> ExecutableQuery::GetFunctionNames() const {
  std::vector<std::string> function_names{};
  for (const auto &f : fragments_) {
    const auto &frag_functions = f->GetFunctions();
    function_names.insert(function_names.end(), frag_functions.cbegin(), frag_functions.cend());
  }
  return function_names;
}

std::vector<const vm::FunctionInfo *> ExecutableQuery::GetFunctionMetadata() const {
  std::vector<const vm::FunctionInfo *> function_meta{};
  for (const auto &f : fragments_) {
    const auto function_names = f->GetFunctions();
    for (const auto &function_name : function_names) {
      auto meta = f->GetFunctionMetadata(function_name);
      NOISEPAGE_ASSERT(meta.has_value(), "Broken invariant");
      function_meta.push_back(meta.value());
    }
  }
  return function_meta;
}

std::vector<ast::Decl *> ExecutableQuery::GetDecls() const {
  std::vector<ast::Decl *> decls{};
  for (const auto &f : fragments_) {
    const auto &frag_decls = f->GetFile()->Declarations();
    decls.insert(decls.end(), frag_decls.cbegin(), frag_decls.cend());
  }
  return decls;
}

}  // namespace noisepage::execution::compiler
