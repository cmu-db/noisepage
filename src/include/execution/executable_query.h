#pragma once
#include <memory>
#include <string>
#include <utility>

#include "brain/operating_unit.h"
#include "common/managed_pointer.h"
#include "common/strong_typedef.h"
#include "execution/ast/context.h"
#include "execution/exec_defs.h"
#include "execution/vm/module.h"

namespace terrier::planner {
class AbstractPlanNode;
}

namespace terrier::runner {
class MiniRunners;
class MiniRunners_SEQ0_OutputRunners_Benchmark;
}  // namespace terrier::runner

namespace terrier::execution {

namespace exec {
class ExecutionContext;
}

namespace vm {
enum class ExecutionMode : uint8_t;
class Module;
}  // namespace vm

namespace util {
class Region;
}

/**
 * ExecutableQuery abstracts the TPL code generation and compilation process. The result is an object that can be
 * invoked multiple times with multiple ExecutionContexts in multiple execution modes for as long its generated code is
 * valid (i.e. the objects to which it refers still exist).
 */
class ExecutableQuery {
 public:
  /**
   * Construct an executable query that maintains necessary state to be reused with multiple ExecutionContexts. It is up
   * to the owner to invalidate this object in the event that its references are no longer valid (schema change).
   * @param physical_plan output from the optimizer
   * @param exec_ctx execution context to use for code generation. Note that this execution context need not be the one
   * used for Run.
   */
  ExecutableQuery(common::ManagedPointer<planner::AbstractPlanNode> physical_plan,
                  common::ManagedPointer<exec::ExecutionContext> exec_ctx);

  /**
   * Construct and compile an executable TPL program from file or source
   *
   * @param contents Name of the file or TPL program
   * @param exec_ctx context to execute
   * @param is_file Whether load from file
   */
  ExecutableQuery(const std::string &contents, common::ManagedPointer<exec::ExecutionContext> exec_ctx, bool is_file);

  /**
   *
   * @param exec_ctx execution context to use for execution. Note that this execution context need not be the one used
   * for construction/codegen.
   * @param mode execution mode to use
   */
  void Run(common::ManagedPointer<exec::ExecutionContext> exec_ctx, vm::ExecutionMode mode);

  /**
   * @note function should only be used from test
   * @returns the query name
   */
  const std::string &GetQueryName() const { return query_name_; }

  /**
   * @returns the query identifier
   */
  query_id_t GetQueryId() const { return query_id_; }

  /**
   * @returns Pipeline Units
   */
  common::ManagedPointer<brain::PipelineOperatingUnits> GetPipelineOperatingUnits() {
    return common::ManagedPointer(pipeline_operating_units_);
  }

 private:
  static std::string GetFileName(const std::string &path) {
    std::size_t size = path.size();
    std::size_t found = path.find_last_of("/\\");
    return path.substr(found + 1, size - found - 5);
  }

  /**
   * Set Pipeline Operating Units for use by mini_runners
   * @param units Pipeline Operating Units
   */
  void SetPipelineOperatingUnits(std::unique_ptr<brain::PipelineOperatingUnits> &&units) {
    pipeline_operating_units_ = std::move(units);
  }

  // TPL bytecodes for this query.
  std::unique_ptr<vm::Module> tpl_module_ = nullptr;

  std::function<int64_t(exec::ExecutionContext *)> main_;

  // Memory region and AST context from the code generation stage that need to stay alive as long as the TPL module will
  // be executed. Direct access to these objects is likely unneeded from this class, we just want to tie the life cycles
  // together.
  std::unique_ptr<util::Region> region_;
  std::unique_ptr<ast::Context> ast_ctx_;
  std::unique_ptr<brain::PipelineOperatingUnits> pipeline_operating_units_;

  std::string query_name_;
  query_id_t query_id_;
  static std::atomic<query_id_t> query_identifier;

  // MiniRunners needs to set query_identifier
  friend class terrier::runner::MiniRunners;
  friend class terrier::runner::MiniRunners_SEQ0_OutputRunners_Benchmark;
};
}  // namespace terrier::execution
