#pragma once

#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "common/macros.h"
#include "common/managed_pointer.h"
#include "execution/ast/ast_fwd.h"
#include "execution/exec_defs.h"
#include "execution/vm/vm_defs.h"

namespace noisepage {
namespace selfdriving {
class PipelineOperatingUnits;
class PilotUtil;
}  // namespace selfdriving

namespace execution {
namespace exec {
class ExecutionContext;
class ExecutionSettings;
}  // namespace exec

namespace sema {
class ErrorReporter;
}  // namespace sema

namespace util {
class Region;
}  // namespace util

namespace vm {
class Module;
}  // namespace vm
}  // namespace execution

namespace planner {
class AbstractPlanNode;
}  // namespace planner

namespace runner {
class ExecutionRunners;
class ExecutionRunners_SEQ0_OutputRunners_Benchmark;
class ExecutionRunners_SEQ10_0_IndexInsertRunners_Benchmark;
}  // namespace runner

}  // namespace noisepage

namespace noisepage::execution::compiler {

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
     * @param teardown_fns The name of the teardown functions in the module, in order.
     * @param module The module that contains the functions.
     */
    Fragment(std::vector<std::string> &&functions, std::vector<std::string> &&teardown_fns,
             std::unique_ptr<vm::Module> module);

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

    std::vector<std::string> teardown_fn_;

    // The module.
    std::unique_ptr<vm::Module> module_;
  };

  /**
   * Create a query object.
   * @param plan The physical plan.
   * @param exec_settings The execution settings used for this query.
   */
  ExecutableQuery(const planner::AbstractPlanNode &plan, const exec::ExecutionSettings &exec_settings);

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
   * @param pipeline_operating_units The pipeline operating units that were generated with the fragments.
   */
  void Setup(std::vector<std::unique_ptr<Fragment>> &&fragments, std::size_t query_state_size,
             std::unique_ptr<selfdriving::PipelineOperatingUnits> pipeline_operating_units);

  /**
   * Execute the query.
   * @param exec_ctx The context in which to execute the query.
   * @param mode The execution mode to use when running the query. By default, its interpreted.
   */
  void Run(common::ManagedPointer<exec::ExecutionContext> exec_ctx,
           vm::ExecutionMode mode = vm::ExecutionMode::Interpret);

  /**
   * @return The physical plan this executable query implements.
   */
  const planner::AbstractPlanNode &GetPlan() const { return plan_; }

  /**
   * @return The AST context.
   */
  ast::Context *GetContext() { return ast_context_.get(); }

  /** @return The execution settings used for this query. */
  const exec::ExecutionSettings &GetExecutionSettings() const { return exec_settings_; }

  /** @return The pipeline operating units that were used to generate this query. Setup must have been called! */
  common::ManagedPointer<selfdriving::PipelineOperatingUnits> GetPipelineOperatingUnits() const {
    return common::ManagedPointer(pipeline_operating_units_);
  }

  /** @return The Query Identifier */
  query_id_t GetQueryId() { return query_id_; }

  /** @param query_text The SQL string for this query */
  void SetQueryText(common::ManagedPointer<const std::string> query_text) { query_text_ = query_text; }

  /** @return The SQL query string */
  common::ManagedPointer<const std::string> GetQueryText() { return query_text_; }

 private:
  // The plan.
  const planner::AbstractPlanNode &plan_;
  // The execution settings used for code generation.
  const exec::ExecutionSettings &exec_settings_;
  std::unique_ptr<util::Region> errors_region_;
  std::unique_ptr<util::Region> context_region_;
  // The AST error reporter.
  std::unique_ptr<sema::ErrorReporter> errors_;
  // The AST context used to generate the TPL AST.
  std::unique_ptr<ast::Context> ast_context_;
  // The compiled query fragments that make up the query.
  std::vector<std::unique_ptr<Fragment>> fragments_;
  // The query state size.
  std::size_t query_state_size_;

  // The pipeline operating units that were generated as part of this query.
  std::unique_ptr<selfdriving::PipelineOperatingUnits> pipeline_operating_units_;

  // For mini_runners.cpp

  /** Legacy constructor that creates a hardcoded fragment with main(ExecutionContext*)->int32. */
  ExecutableQuery(const std::string &contents, common::ManagedPointer<exec::ExecutionContext> exec_ctx, bool is_file,
                  size_t query_state_size, const exec::ExecutionSettings &exec_settings);
  /**
   * Set Pipeline Operating Units for use by mini_runners
   * @param units Pipeline Operating Units
   */
  void SetPipelineOperatingUnits(std::unique_ptr<selfdriving::PipelineOperatingUnits> &&units);

  std::string query_name_;
  query_id_t query_id_;
  static std::atomic<query_id_t> query_identifier;
  common::ManagedPointer<const std::string> query_text_;

  // MiniRunners needs to set query_identifier and pipeline_operating_units_.
  friend class noisepage::runner::ExecutionRunners;
  friend class noisepage::runner::ExecutionRunners_SEQ0_OutputRunners_Benchmark;
  friend class noisepage::selfdriving::PilotUtil;
  friend class noisepage::runner::ExecutionRunners_SEQ10_0_IndexInsertRunners_Benchmark;
};

}  // namespace noisepage::execution::compiler
