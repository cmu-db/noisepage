#pragma once
#include <memory>
#include <utility>

#include "execution/ast/ast_dump.h"
#include "execution/ast/context.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compiler.h"
#include "execution/util/region.h"
#include "execution/vm/bytecode_generator.h"
#include "execution/vm/module.h"
#include "loggers/execution_logger.h"

namespace terrier::execution {

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
   *
   * @param exec_ctx execution context to use for execution. Note that this execution context need not be the one used
   * for construction/codegen.
   * @param mode execution mode to use
   */
  void Run(common::ManagedPointer<exec::ExecutionContext> exec_ctx, vm::ExecutionMode mode);

 private:
  // TPL bytecodes for this query.
  std::unique_ptr<vm::Module> tpl_module_ = nullptr;

  // Memory region and AST context from the code generation stage that need to stay alive as long as the TPL module will
  // be executed. Direct access to these objects is likely unneeded from this class, we just want to tie the life cycles
  // together.
  std::unique_ptr<util::Region> region_;
  std::unique_ptr<ast::Context> ast_ctx_;
};
}  // namespace terrier::execution