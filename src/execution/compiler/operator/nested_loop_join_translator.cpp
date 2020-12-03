#include "execution/compiler/operator/nested_loop_join_translator.h"

#include "execution/compiler/compilation_context.h"
#include "execution/compiler/if.h"
#include "execution/compiler/pipeline.h"
#include "execution/compiler/work_context.h"
#include "planner/plannodes/nested_loop_join_plan_node.h"

namespace noisepage::execution::compiler {

NestedLoopJoinTranslator::NestedLoopJoinTranslator(const planner::NestedLoopJoinPlanNode &plan,
                                                   CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline, selfdriving::ExecutionOperatingUnitType::DUMMY) {
  NOISEPAGE_ASSERT(plan.GetChildrenSize() == 2, "NLJ expected to have only two children.");

  // In a nested loop, only the outer most loop determines the parallelism level.
  // So disable the parallelism check until the last child.
  pipeline->SetParallelCheck(false);
  compilation_context->Prepare(*plan.GetChild(0), pipeline);

  // Re-enable the parallelism check for the outer most loop.
  pipeline->SetParallelCheck(true);
  compilation_context->Prepare(*plan.GetChild(1), pipeline);

  // Prepare join condition.
  if (const auto join_predicate = plan.GetJoinPredicate(); join_predicate != nullptr) {
    compilation_context->Prepare(*join_predicate);
  }
}

void NestedLoopJoinTranslator::PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const {
  if (const auto join_predicate = GetNLJPlan().GetJoinPredicate(); join_predicate != nullptr) {
    If cond(function, context->DeriveValue(*join_predicate, this));
    {
      // Valid tuple. Push to next operator in pipeline.
      context->Push(function);
    }
    cond.EndIf();
  } else {
    // No join predicate. Push to next operator in pipeline.
    context->Push(function);
  }
}

}  // namespace noisepage::execution::compiler
