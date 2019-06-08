#pragma once

#include <string>

#include "execution/compiler/code_context.h"
#include "execution/compiler/query_state.h"
#include "execution/util/region.h"
#include "execution/exec/execution_context.h"
#include "catalog/catalog.h"

namespace terrier::planner {
class AbstractPlanNode;
}

namespace tpl::ast {
class BlockStmt;
}

namespace tpl::compiler {

/**
 * Query to execute.
 */
class Query {
 public:
  /**
   * Constructor
   * @param node plan node to execute
   */
  explicit Query(const terrier::planner::AbstractPlanNode &node, exec::ExecutionContext * exec_ctx)
      : node_(node),
        region_("QueryRegion"),
        code_ctx_(&region_),
        query_state_(ast::Identifier(GetQueryStateName().c_str())),
        compiled_fn_(nullptr),
        exec_ctx_{exec_ctx},
        final_schema_(std::vector<terrier::catalog::Schema::Column>{}, std::unordered_map<u32, u32>{}){}

  /// Destructor
  ~Query() = default;

  /**
   * @return the plan node
   */
  const terrier::planner::AbstractPlanNode &GetPlan() { return node_; }

  /**
   * @return the code context
   */
  CodeContext *GetCodeContext() { return &code_ctx_; }

  /**
   * @return the query state object
   */
  QueryState *GetQueryState() { return &query_state_; }

  /**
   * @return the region used for allocation
   */
  util::Region *GetRegion() { return &region_; }

  /**
   * @return name of the query state variable
   */
  const std::string &GetQueryStateName() { return name_qs; }

  /**
   * @return name of the query state struct
   */
  const std::string &GetQueryStateStructName() { return name_qs_struct; }

  /**
   * @return name of the init function
   */
  const std::string &GetQueryInitName() { return name_qinit; }

  /**
   * @return name of the produce function
   */
  const std::string &GetQueryProduceName() { return name_qproduce; }

  /**
   * @return name of the teardown function
   */
  const std::string &GetQueryTeardownName() { return name_qteardown; }

  /**
   * Return the execution context
   */
  exec::ExecutionContext * GetExecutionContext() { return exec_ctx_; }

  /**
   * Return the final schema
   */
  exec::FinalSchema * GetFinalSchema() { return &final_schema_; }

  /**
   * TODO(Amadou): rename to SetCompiledFile?
   * Sets the final compiled file
   * @param fn compiled file
   */
  void SetCompiledFunction(ast::File *fn) { compiled_fn_ = fn; }

  /**
   * TODO(Amadou): rename to GetCompiledFile?
   * @return the compiled file.
   */
  ast::File *GetCompiledFunction() { return compiled_fn_; }

 private:
  // TODO(WAN): in principle we can NewIdentifier() all these but then reading the AST dump is a nightmare
  std::string name_qs = "query_state";
  std::string name_qinit = "query_init";
  std::string name_qproduce = "query_produce";
  std::string name_qteardown = "query_teardown";
  std::string name_qs_struct = "query_state_struct";

  const terrier::planner::AbstractPlanNode &node_;
  util::Region region_;
  CodeContext code_ctx_;
  QueryState query_state_;
  ast::File *compiled_fn_;
  exec::ExecutionContext * exec_ctx_;
  exec::FinalSchema final_schema_;
};

}  // namespace tpl::compiler
