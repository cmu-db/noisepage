#include "execution/compiler/operator/cte_scan_translator.h"

#include "execution/compiler/codegen.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/if.h"
#include "execution/compiler/loop.h"
#include "execution/compiler/operator/cte_scan_leader_translator.h"
#include "execution/compiler/work_context.h"
#include "parser/expression/constant_value_expression.h"

namespace noisepage::execution::compiler {

CteScanTranslator::CteScanTranslator(const planner::CteScanPlanNode &plan, CompilationContext *compilation_context,
                                     Pipeline *pipeline)
    : SeqScanTranslator(plan, compilation_context, pipeline) {
  // If it has no children, plan node is just a reader and need not do anything
  if (plan.GetChildrenSize() > 0) {
    NOISEPAGE_ASSERT(plan.GetChildrenSize() == 1, "CteScanPlanNode must have only one child");
    compilation_context->Prepare(*(plan.GetChild(0)), pipeline);
  }
}

}  // namespace noisepage::execution::compiler
