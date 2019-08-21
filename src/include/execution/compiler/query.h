#pragma once

#include <string>

#include "catalog/catalog.h"
#include "execution/ast/ast.h"
#include "execution/ast/ast_node_factory.h"
#include "execution/ast/context.h"
#include "execution/exec/execution_context.h"
#include "execution/sema/error_reporter.h"
#include "execution/util/region.h"

namespace terrier::planner {
class AbstractPlanNode;
}

namespace terrier::execution::ast {
class BlockStmt;
}

namespace terrier::execution::compiler {

/**
 * Query to execute.
 */
class Query {
 public:
  /**
   * Constructor
   * @param node plan node to execute
   */
  Query(const terrier::planner::AbstractPlanNode &node, exec::ExecutionContext *exec_ctx)
      : node_(node),
        region_("QueryRegion"),
        error_reporter_(&region_),
        ast_ctx_(&region_, &error_reporter_),
        factory_(&region_),
        exec_ctx_{exec_ctx} {}

  /**
   * Destructor
   */
  ~Query() = default;

  /**
   * @return the plan node
   */
  const terrier::planner::AbstractPlanNode &GetPlan() { return node_; }

  /**
   * @return the region used for allocation
   */
  util::Region *GetRegion() { return &region_; }

  /**
   * @return the ast context
   */
  ast::Context *GetAstContext() { return &ast_ctx_; }

  /**
   * @return the ast factory
   */
  ast::AstNodeFactory *GetFactory() { return &factory_; }

  /**
   * @return the error reporter
   */
  sema::ErrorReporter *GetReporter() { return &error_reporter_; }

  /**
   * @return the execution context
   */
  exec::ExecutionContext *GetExecCtx() { return exec_ctx_; }

  /**
   * Sets the final compiled file
   * @param file the compiled file
   */
  void SetCompiledFile(ast::File *file) { compiled_file_ = file; }

  /**
   * @return the compiled file.
   */
  ast::File *GetCompiledFile() { return compiled_file_; }

 private:
  const terrier::planner::AbstractPlanNode &node_;
  util::Region region_;
  sema::ErrorReporter error_reporter_;
  ast::Context ast_ctx_;
  ast::AstNodeFactory factory_;
  ast::File *compiled_file_{nullptr};
  exec::ExecutionContext *exec_ctx_;
};

}  // namespace terrier::execution::compiler
