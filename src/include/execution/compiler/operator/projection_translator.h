#pragma once

#include "execution/compiler/operator/operator_translator.h"

namespace terrier::planner {
class ProjectionPlanNode;
}  // namespace terrier::planner

namespace terrier::execution::compiler {

/**
 * Translator for projections.
 */
class ProjectionTranslator : public OperatorTranslator {
 public:
  /**
   * Create a translator for the given plan.
   * @param plan The plan.
   * @param compilation_context The context this translator belongs to.
   * @param pipeline The pipeline this translator is participating in.
   */
  ProjectionTranslator(const planner::ProjectionPlanNode &plan, CompilationContext *compilation_context,
                       Pipeline *pipeline);

  /**
   * Push the context through this operator to the next in the pipeline.
   * @param context The context.
   * @param function The pipeline generating function.
   */
  void PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const override;

  /**
   * Projections do not produce columns from base tables.
   */
  ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override {
    UNREACHABLE("Projections do not produce columns from base tables.");
  }
};

}  // namespace terrier::execution::compiler
