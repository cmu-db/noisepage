#include "execution/compiler/operator/projection_translator.h"

#include "execution/compiler/compilation_context.h"
#include "execution/compiler/work_context.h"
#include "planner/plannodes/projection_plan_node.h"

namespace noisepage::execution::compiler {

// The majority of work for projections are performed during expression
// evaluation. In the context of projections, expressions are derived when
// requesting an output from the expression.

ProjectionTranslator::ProjectionTranslator(const planner::ProjectionPlanNode &plan,
                                           CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline, selfdriving::ExecutionOperatingUnitType::PROJECTION) {
  switch (plan.GetChildrenSize()) {
    case 0: {
      // This should only happen for SELECT 1; type of situations.
      pipeline->RegisterSource(this, Pipeline::Parallelism::Serial);
      break;
    }
    case 1: {
      // Projections are expected to have one child, unless you have a SELECT 1; type of situation.
      // Prepare children for codegen.
      compilation_context->Prepare(*plan.GetChild(0), pipeline);
      break;
    }
    default:
      UNREACHABLE("Projections should have either 0 or 1 child!");
  }
}

void ProjectionTranslator::PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const {
  context->Push(function);
}

}  // namespace noisepage::execution::compiler
