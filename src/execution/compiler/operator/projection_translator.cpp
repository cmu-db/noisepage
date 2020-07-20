#include "execution/compiler/operator/projection_translator.h"

#include "execution/compiler/compilation_context.h"
#include "execution/compiler/work_context.h"
#include "planner/plannodes/projection_plan_node.h"

namespace terrier::execution::compiler {

// The majority of work for projections are performed during expression
// evaluation. In the context of projections, expressions are derived when
// requesting an output from the expression.

ProjectionTranslator::ProjectionTranslator(const planner::ProjectionPlanNode &plan,
                                           CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline, brain::ExecutionOperatingUnitType::PROJECTION) {
  // Projections are expected to have one child, unless you have a SELECT 1; type of situation.
  if (plan.GetChildrenSize() == 1) {
    // Prepare children for codegen.
    compilation_context->Prepare(*plan.GetChild(0), pipeline);
  }
}

void ProjectionTranslator::PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const {
  context->Push(function);
}

}  // namespace terrier::execution::compiler
