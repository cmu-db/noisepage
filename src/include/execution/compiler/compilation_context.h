#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "execution/compiler/codegen.h"
#include "execution/compiler/translator_factory.h"

#include "execution/util/region.h"

namespace terrier::parser {
class AbstractExpression;
}

namespace terrier::planner {
class AbstractPlanNode;
}

namespace tpl::compiler {

// TODO(WAN): parameter cache would be nice
class ExecutionConsumer;
class Query;

class CompilationContext {
 public:
  CompilationContext(Query *query, ExecutionConsumer *consumer);

  void GeneratePlan(Query *query);

  u32 RegisterPipeline(Pipeline *pipeline);

  ExecutionConsumer *GetExecutionConsumer();
  CodeGen *GetCodeGen();
  util::Region *GetRegion();

  void Prepare(const terrier::planner::AbstractPlanNode &op, Pipeline *pipeline);
  void Prepare(const terrier::parser::AbstractExpression &exp);

  OperatorTranslator *GetTranslator(const terrier::planner::AbstractPlanNode &op) const;
  ExpressionTranslator *GetTranslator(const terrier::parser::AbstractExpression &ex) const;

  Query *GetQuery() { return query_; }

 private:
  Query *query_;
  ExecutionConsumer *consumer_;
  CodeGen codegen_;

  std::vector<Pipeline *> pipelines_;
  std::unordered_map<const terrier::parser::AbstractExpression *, ExpressionTranslator *> ex_translators_;
  std::unordered_map<const terrier::planner::AbstractPlanNode *, OperatorTranslator *> op_translators_;
};

}  // namespace tpl::compiler
