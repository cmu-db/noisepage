#pragma once

#include "execution/util/common.h"
#include "execution/util/region.h"
#include "execution/compiler/query.h"
#include "execution/compiler/query_state.h"
#include "parser/expression/abstract_expression.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "execution/compiler/pipeline.h"
#include "execution/compiler/execution_consumer.h"
#include "execution/compiler/expression/expression_translator.h"
#include "execution/compiler/operator/operator_translator.h"



namespace tpl::compiler {
/**
 * Compiler is the main class that performs compilation through GeneratePlan.
 */
class Compiler {
 public:
  /**
   * Constructor
   * @param query query to compiler
   * @param consumer consumer for upper layers
   */
  Compiler(Query *query);


  void Compile();

  void MakePipelines(const terrier::planner::AbstractPlanNode & op, Pipeline * curr_pipeline);

 private:
  Query *query_;
  QueryState *query_state_;
  ExecutionConsumer consumer_;
  CodeGen codegen_;

  std::vector<Pipeline*> pipelines_;
  std::unordered_map<const terrier::parser::AbstractExpression *, ExpressionTranslator *> ex_translators_;
  std::unordered_map<const terrier::planner::AbstractPlanNode *, OperatorTranslator *> op_translators_;
  sema::ErrorReporter error_reporter_;
};

}