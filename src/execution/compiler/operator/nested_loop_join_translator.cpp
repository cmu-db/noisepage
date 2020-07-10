#include "execution/compiler/operator/nested_loop_join_translator.h"

#include "execution/compiler/compilation_context.h"
#include "execution/compiler/if.h"
#include "execution/compiler/pipeline.h"
#include "execution/compiler/work_context.h"
#include "planner/plannodes/index_scan_plan_node.h"
#include "planner/plannodes/nested_loop_join_plan_node.h"
#include "planner/plannodes/seq_scan_plan_node.h"

namespace terrier::execution::compiler {

NestedLoopJoinTranslator::NestedLoopJoinTranslator(const planner::NestedLoopJoinPlanNode &plan,
                                                   CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline, brain::ExecutionOperatingUnitType::NL_JOIN) {
  TERRIER_ASSERT(plan.GetChildrenSize() == 2, "NLJ expected to have only two children.");

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

bool NestedLoopJoinTranslator::IsOidProvider(const planner::AbstractPlanNode &plan, catalog::col_oid_t oid) const {
  switch (plan.GetPlanNodeType()) {
    case planner::PlanNodeType::SEQSCAN: {
      const auto &node = dynamic_cast<const planner::SeqScanPlanNode &>(plan);
      const auto &oids = node.GetColumnOids();
      bool found = std::find(oids.cbegin(), oids.cend(), oid) != oids.cend();
      return found;
    }
    case planner::PlanNodeType::INDEXSCAN: {
      const auto &node = dynamic_cast<const planner::IndexScanPlanNode &>(plan);
      const auto &oids = node.GetColumnOids();
      bool found = std::find(oids.cbegin(), oids.cend(), oid) != oids.cend();
      return found;
    }
    default:
      break;
  }
  return false;
}

ast::Expr *NestedLoopJoinTranslator::GetTableColumn(catalog::col_oid_t col_oid) const {
  for (auto i = 0; i < 2; ++i) {
    const auto &plan = *GetPlan().GetChild(i);
    if (IsOidProvider(plan, col_oid)) {
      return LookupPreparedChildTranslator(plan)->GetTableColumn(col_oid);
    }
  }
  UNREACHABLE("Unable to find a provider for this OID.");
}

}  // namespace terrier::execution::compiler
