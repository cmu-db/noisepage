#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "execution/compiler/codegen.h"
#include "execution/compiler/translator_factory.h"

namespace terrier::parser {
class AbstractExpression;
}

namespace terrier::planner {
class AbstractPlanNode;
}

namespace tpl::compiler {

// TODO(WAN): parameter cache would be nice
class CodeContext;
class ExecutionConsumer;
class Query;
class QueryState;
class Pipeline;
class OperatorTranslator;
class ExpressionTranslator;

class CompilationContext {
 public:
  CompilationContext(Query *query, ExecutionConsumer *consumer);

  void GeneratePlan(Query *query);

  void Prepare(const terrier::planner::AbstractPlanNode &op, Pipeline *pipeline);
  void Prepare(const terrier::parser::AbstractExpression &ex);

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