#pragma once

#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "common/macros.h"
#include "common/managed_pointer.h"
#include "execution/ast/ast_fwd.h"
#include "execution/vm/vm_defs.h"

namespace terrier::execution::exec {
class ExecutionContext;
}  // namespace terrier::execution::exec

namespace terrier::execution::sema {
class ErrorReporter;
}  // namespace terrier::execution::sema

namespace terrier::planner {
class AbstractPlanNode;
}  // namespace terrier::planner

namespace terrier::execution::vm {
class Module;
}  // namespace terrier::execution::vm

namespace terrier::execution::codegen {

/**
 * An compiled and executable query object.
 */
class ExecutableQuery {
 public:
  /**
   * A self-contained unit of execution that represents a chunk of a larger query. All executable
   * queries are composed of at least one fragment.
   */
  class Fragment {
   public:
    /**
     * Construct a fragment composed of the given functions from the given module.
     * @param functions The name of the functions to execute, in order.
     * @param module The module that contains the functions.
     */
    Fragment(std::vector<std::string> &&functions, std::unique_ptr<vm::Module> module);

    /**
     * Destructor.
     */
    ~Fragment();

    /**
     * Run this fragment using the provided opaque query state object.
     * @param query_state The query state.
     * @param mode The execution mode to run the query with.
     */
    void Run(std::byte query_state[], vm::ExecutionMode mode) const;

    /**
     * @return True if this fragment is compiled and executable.
     */
    bool IsCompiled() const { return module_ != nullptr; }

   private:
    // The functions that must be run (in the provided order) to execute this
    // query fragment.
    std::vector<std::string> functions_;
    // The module.
    std::unique_ptr<vm::Module> module_;
  };

  /**
   * Create a query object.
   * @param plan The physical plan.
   * @param exec_ctx The execution context for code generation, can differ from the one used in Run.
   */
  ExecutableQuery(const planner::AbstractPlanNode &plan, common::ManagedPointer<exec::ExecutionContext> exec_ctx);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(ExecutableQuery);

  /**
   * Destructor.
   */
  ~ExecutableQuery();

  /**
   * Setup the compiled query using the provided fragments.
   * @param fragments The fragments making up the query. These are provided as a vector in the order
   *                  they're to be executed.
   * @param query_state_size The size of the state structure this query needs. This value is
   *                         represented in bytes.
   */
  void Setup(std::vector<std::unique_ptr<Fragment>> &&fragments, std::size_t query_state_size);

  /**
   * Execute the query.
   * @param exec_ctx The context in which to execute the query.
   * @param mode The execution mode to use when running the query. By default, its interpreted.
   */
  void Run(exec::ExecutionContext *exec_ctx, vm::ExecutionMode mode = vm::ExecutionMode::Interpret);

  /**
   * @return The physical plan this executable query implements.
   */
  const planner::AbstractPlanNode &GetPlan() const { return plan_; }

  /**
   * @return The AST context.
   */
  ast::Context *GetContext() { return ast_context_.get(); }

  /** @return The execution context to be used for code generation. */
  common::ManagedPointer<exec::ExecutionContext> GetExecutionContextForCompilation() const {
    return exec_ctx_compilation_;
  }

 private:
  // The plan.
  const planner::AbstractPlanNode &plan_;
  // The execution context used for code generation.
  common::ManagedPointer<exec::ExecutionContext> exec_ctx_compilation_;
  // The AST error reporter.
  std::unique_ptr<sema::ErrorReporter> errors_;
  // The AST context used to generate the TPL AST.
  std::unique_ptr<ast::Context> ast_context_;
  // The compiled query fragments that make up the query.
  std::vector<std::unique_ptr<Fragment>> fragments_;
  // The query state size.
  std::size_t query_state_size_;
};

}  // namespace terrier::execution::codegen
