#pragma once

#include <unordered_map>

#include "execution/ast/ast.h"
#include "execution/compiler/expression/expression_translator.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/translator_factory.h"
#include "execution/compiler/pipeline.h"

namespace terrier {
namespace parser { class AbstractExpression; }
namespace planner { class AbstractPlanNode; }
}

namespace tpl::compiler {

class ExecutionConsumer;
class Pipeline;
class Query;

class CompilationContext {
 public:
  CompilationContext(Query *query, ExecutionConsumer *consumer);
  void GeneratePlan(Query *query);

  void Prepare(const terrier::parser::AbstractExpression &expr);
  void Prepare(const terrier::planner::AbstractPlanNode &node, Pipeline *pipeline);

  void Produce(const terrier::planner::AbstractPlanNode &node);

  ExpressionTranslator *GetTranslator(const terrier::parser::AbstractExpression &expr) const;
  OperatorTranslator *GetTranslator(const terrier::planner::AbstractPlanNode &expr) const;

  u32 RegisterPipeline(Pipeline *pipeline) {
    auto pos = pipelines_.size();
    pipelines_.emplace_back(pipeline);
    return pos;
  }

  bool IsLastPipeline(const Pipeline &pipeline) { return pipeline.GetId() == 0; }

 private:
  ExecutionConsumer *consumer_;
  CodeGen codegen_;
  TranslatorFactory translator_factory_;

  std::vector<Pipeline *> pipelines_;
  std::unordered_map<const terrier::planner::AbstractPlanNode *, std::unique_ptr<OperatorTranslator>> op_translators_;
  std::unordered_map<const terrier::parser::AbstractExpression *, std::unique_ptr<ExpressionTranslator>> ex_translators_;
};

}