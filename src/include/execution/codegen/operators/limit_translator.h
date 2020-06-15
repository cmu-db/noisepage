#pragma once

#include <vector>

#include "execution/codegen/ast_fwd.h"
#include "execution/codegen/operators/operator_translator.h"
#include "execution/codegen/pipeline.h"
#include "execution/codegen/state_descriptor.h"

namespace terrier::parser {
class LimitPlanNode;
}  // namespace terrier::parser

namespace terrier::execution::codegen {

class FunctionBuilder;

/**
 * A translator for limits and offsets.
 */
class LimitTranslator : public OperatorTranslator {
 public:
  /**
   * Create a new translator for the given limit plan. The compilation occurs within the
   * provided compilation context and the operator is participating in the provided pipeline.
   * @param plan The plan.
   * @param compilation_context The context of compilation this translation is occurring in.
   * @param pipeline The pipeline this operator is participating in.
   */
  LimitTranslator(const planner::LimitPlanNode &plan, CompilationContext *compilation_context, Pipeline *pipeline);

  /**
   * Initialize the tuple counter in the pipeline local state.
   * @param pipeline The pipeline that's being generated.
   * @param function The pipeline function generator.
   */
  void InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * Implement the limit's logic.
   * @param context The context of work.
   * @param function The pipeline function generator.
   */
  void PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const override;

  /**
   * Limits never touch raw table data.
   */
  ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override {
    UNREACHABLE("NLJ are never the root of a plan and, thus, cannot be launched in parallel.");
  }

 private:
  // The tuple counter.
  StateDescriptor::Entry tuple_count_;
};

}  // namespace terrier::execution::codegen
