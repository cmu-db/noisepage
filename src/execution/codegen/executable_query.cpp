#include "execution/sql/codegen/executable_query.h"

#include <algorithm>

#include "common/exception.h"
#include "execution/ast/context.h"
#include "execution/sql/execution_context.h"
#include "execution/vm/module.h"
#include "logging/logger.h"
#include "sema/error_reporter.h"

namespace terrier::execution::codegen {

//===----------------------------------------------------------------------===//
//
// Executable Query Fragment
//
//===----------------------------------------------------------------------===//

ExecutableQuery::Fragment::Fragment(std::vector<std::string> &&functions, std::unique_ptr<vm::Module> module)
    : functions_(std::move(functions)), module_(std::move(module)) {}

ExecutableQuery::Fragment::~Fragment() = default;

void ExecutableQuery::Fragment::Run(byte query_state[], vm::ExecutionMode mode) const {
  using Function = std::function<void(void *)>;
  for (const auto &func_name : functions_) {
    Function func;
    if (!module_->GetFunction(func_name, mode, func)) {
      throw Exception(ExceptionType::Execution,
                      fmt::format("Could not find function '{}' in query fragment", func_name));
    }
    func(query_state);
  }
}

//===----------------------------------------------------------------------===//
//
// Executable Query
//
//===----------------------------------------------------------------------===//

ExecutableQuery::ExecutableQuery(const planner::AbstractPlanNode &plan)
    : plan_(plan),
      errors_(std::make_unique<sema::ErrorReporter>()),
      ast_context_(std::make_unique<ast::Context>(errors_.get())),
      query_state_size_(0) {}

// Needed because we forward-declare classes used as template types to std::unique_ptr<>
ExecutableQuery::~ExecutableQuery() = default;

void ExecutableQuery::Setup(std::vector<std::unique_ptr<Fragment>> &&fragments, const std::size_t query_state_size) {
  TPL_ASSERT(
      std::all_of(fragments.begin(), fragments.end(), [](const auto &fragment) { return fragment->IsCompiled(); }),
      "All query fragments are not compiled!");
  TPL_ASSERT(query_state_size >= sizeof(void *),
             "Query state must be large enough to store at least an ExecutionContext pointer.");

  fragments_ = std::move(fragments);
  query_state_size_ = query_state_size;

  LOG_INFO("Query has {} fragment{} with {}-byte query state.", fragments_.size(), fragments_.size() > 1 ? "s" : "",
           query_state_size_);
}

void ExecutableQuery::Run(ExecutionContext *exec_ctx, vm::ExecutionMode mode) {
  // First, allocate the query state and move the execution context into it.
  auto query_state = std::make_unique<byte[]>(query_state_size_);
  *reinterpret_cast<ExecutionContext **>(query_state.get()) = exec_ctx;

  // Now run through fragments.
  for (const auto &fragment : fragments_) {
    fragment->Run(query_state.get(), mode);
  }
}

}  // namespace terrier::execution::codegen
