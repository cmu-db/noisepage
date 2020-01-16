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

class ExecutableQuery {
 public:
  ExecutableQuery(common::ManagedPointer<planner::AbstractPlanNode> physical_plan,
                  common::ManagedPointer<exec::ExecutionContext> exec_ctx);

  void Run(common::ManagedPointer<exec::ExecutionContext> exec_ctx, vm::ExecutionMode mode);

 private:
  // TPL bytecodes for this query
  std::unique_ptr<vm::Module> tpl_module_;

  // Memory region and AST context from the code generation stage that need to stay alive as long as the TPL module will
  // be executed. Direct access to these objects is likely unneeded from this class, we just want to tie the life cycles
  // together.
  std::unique_ptr<util::Region> region_;
  std::unique_ptr<ast::Context> ast_ctx_;
};
}  // namespace terrier::execution