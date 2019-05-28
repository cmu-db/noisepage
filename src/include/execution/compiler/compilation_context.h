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

/**
 * CompilationContext is the main class that performs compilation through GeneratePlan.
 */
class CompilationContext {
 public:
  /**
   * Constructor
   * @param query query to compiler
   * @param consumer consumer for upper layers
   */
  CompilationContext(Query *query, ExecutionConsumer *consumer);

  /**
   * Generates the TPL ast for execution.
   * @param query query to execute.
   */
  void GeneratePlan(Query *query);

  /**
   * @param pipeline pipeline to add
   * @return index of the added pipeline
   */
  u32 RegisterPipeline(Pipeline *pipeline);

  /**
   * @return the execution consumer
   */
  ExecutionConsumer *GetExecutionConsumer();

  /**
   * @return the code generator
   */
  CodeGen *GetCodeGen();

  /**
   * @return the region used for allocation
   */
  util::Region *GetRegion();

  /**
   * Adds the translator for a plan node.
   * @param op plan node to translate
   * @param pipeline pipeline containing the plan node
   */
  void Prepare(const terrier::planner::AbstractPlanNode &op, Pipeline *pipeline);

  /**
   * Adds the translator for an expression.
   * @param exp expression to translate.
   */
  void Prepare(const terrier::parser::AbstractExpression &exp);

  /**
   * @param op the operator
   * @return the translator for the operator
   */
  OperatorTranslator *GetTranslator(const terrier::planner::AbstractPlanNode &op) const;

  /**
   * @param ex the expression
   * @return the translator for the expression
   */
  ExpressionTranslator *GetTranslator(const terrier::parser::AbstractExpression &ex) const;

  /**
   * @return the query being translated
   */
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
