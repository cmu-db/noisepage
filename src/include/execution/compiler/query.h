#pragma once

#include <memory>

#include "execution/ast/ast.h"
#include "execution/compiler/code_context.h"
#include "execution/compiler/query_state.h"
#include "planner/plannodes/abstract_plan_node.h"

namespace tpl::compiler {

/**
 * Query objects contain their own region. Everything associated with a query, including the compiled TPL AST,
 * is allocated from this region. It gets blown away when the query goes out of scope.
 */
class Query {
 public:
  explicit Query(const terrier::planner::AbstractPlanNode &query_plan)
  : region_("query_region"), query_plan_(query_plan), code_context_(region_), query_state_("QS", region_) {}

  const terrier::planner::AbstractPlanNode &GetPlan() const { return query_plan_; }
  CodeContext &GetCodeContext() { return code_context_; }
  QueryState &GetQueryState() { return query_state_; }

  void SetCompiledQuery(std::shared_ptr<ast::BlockStmt> query) { compiled_query_ = std::move(query); }
  std::shared_ptr<ast::BlockStmt> GetCompiledQuery() const { return compiled_query_; }

 private:
  util::Region region_;
  const terrier::planner::AbstractPlanNode &query_plan_;
  CodeContext code_context_;
  QueryState query_state_;

  std::shared_ptr<ast::BlockStmt> compiled_query_;
};

}  // namespace tpl::compiler
