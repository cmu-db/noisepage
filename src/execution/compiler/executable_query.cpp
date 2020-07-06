#include "execution/compiler/executable_query.h"

#include <algorithm>

#include "brain/operating_unit.h"
#include "common/error/exception.h"
#include "execution/ast/context.h"
#include "execution/exec/execution_context.h"
#include "execution/sema/error_reporter.h"
#include "execution/vm/module.h"
#include "loggers/execution_logger.h"
#include "transaction/transaction_context.h"

namespace terrier::execution::compiler {

//===----------------------------------------------------------------------===//
//
// Executable Query Fragment
//
//===----------------------------------------------------------------------===//

ExecutableQuery::Fragment::Fragment(std::vector<std::string> &&functions, std::vector<std::string> &&teardown_fn,
                                    std::unique_ptr<vm::Module> module)
    : functions_(std::move(functions)), teardown_fn_(std::move(teardown_fn)), module_(std::move(module)) {}

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
      throw EXECUTION_EXCEPTION(fmt::format("Could not find function '{}' in query fragment.", func_name));
    }
    try {
      func(query_state);
    } catch (const AbortException &e) {
      for (const auto &teardown_name : teardown_fn_) {
        if (!module_->GetFunction(teardown_name, mode, &func)) {
          throw EXECUTION_EXCEPTION(fmt::format("Could not find teardown function '{}' in query fragment.", func_name));
        }
        func(query_state);
      }
      return;
    }
  }
}

//===----------------------------------------------------------------------===//
//
// Executable Query
//
//===----------------------------------------------------------------------===//

ExecutableQuery::ExecutableQuery(const planner::AbstractPlanNode &plan, const exec::ExecutionSettings &exec_settings)
    : plan_(plan),
      exec_settings_(exec_settings),
      errors_region_(std::make_unique<util::Region>("errors_region")),
      context_region_(std::make_unique<util::Region>("context_region")),
      errors_(std::make_unique<sema::ErrorReporter>(errors_region_.get())),
      ast_context_(std::make_unique<ast::Context>(context_region_.get(), errors_.get())),
      query_state_size_(0),
      pipeline_operating_units_(nullptr) {}

// Needed because we forward-declare classes used as template types to std::unique_ptr<>
ExecutableQuery::~ExecutableQuery() = default;

void ExecutableQuery::Setup(std::vector<std::unique_ptr<Fragment>> &&fragments, const std::size_t query_state_size,
                            std::unique_ptr<brain::PipelineOperatingUnits> pipeline_operating_units) {
  TERRIER_ASSERT(
      std::all_of(fragments.begin(), fragments.end(), [](const auto &fragment) { return fragment->IsCompiled(); }),
      "All query fragments are not compiled!");
  TERRIER_ASSERT(query_state_size >= sizeof(void *),
                 "Query state must be large enough to store at least an ExecutionContext pointer.");

  fragments_ = std::move(fragments);
  query_state_size_ = query_state_size;
  pipeline_operating_units_ = std::move(pipeline_operating_units);

  EXECUTION_LOG_INFO("Query has {} fragment{} with {}-byte query state.", fragments_.size(),
                     fragments_.size() > 1 ? "s" : "", query_state_size_);
}

void ExecutableQuery::Run(common::ManagedPointer<exec::ExecutionContext> exec_ctx, vm::ExecutionMode mode) {
  // First, allocate the query state and move the execution context into it.
  auto query_state = std::make_unique<byte[]>(query_state_size_);
  *reinterpret_cast<exec::ExecutionContext **>(query_state.get()) = exec_ctx.Get();

  // Now run through fragments.
  for (const auto &fragment : fragments_) {
    fragment->Run(query_state.get(), mode);
  }
}

}  // namespace terrier::execution::compiler
