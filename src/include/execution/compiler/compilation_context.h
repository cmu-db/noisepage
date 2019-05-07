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
  void Prepare(const terrier::parser::AbstractExpression &ex);

  OperatorTranslator *GetTranslator(const terrier::planner::AbstractPlanNode &op) const;
  ExpressionTranslator *GetTranslator(const terrier::parser::AbstractExpression &ex) const;

 private:
  Query *query_;
  ExecutionConsumer *consumer_;
  CodeGen codegen_;

  std::vector<Pipeline *> pipelines_;
  TranslatorFactory translator_factory_;
  std::unordered_map<const terrier::planner::AbstractPlanNode *, std::unique_ptr<OperatorTranslator>> op_translators_;
  std::unordered_map<const terrier::parser::AbstractExpression *, std::unique_ptr<ExpressionTranslator>> ex_translators_;
};

}