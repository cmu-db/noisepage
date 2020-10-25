#include "execution/compiler/operator/limit_translator.h"

#include "execution/compiler/codegen.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/if.h"
#include "execution/compiler/work_context.h"
#include "planner/plannodes/limit_plan_node.h"

namespace noisepage::execution::compiler {

LimitTranslator::LimitTranslator(const planner::LimitPlanNode &plan, CompilationContext *compilation_context,
                                 Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline, brain::ExecutionOperatingUnitType::LIMIT) {
  NOISEPAGE_ASSERT(plan.GetOffset() != 0 || plan.GetLimit() != 0, "Both offset and limit cannot be 0");
  // Limits are serial ... for now.
  pipeline->UpdateParallelism(Pipeline::Parallelism::Serial);
  // Prepare child.
  compilation_context->Prepare(*plan.GetChild(0), pipeline);
  // Register state.
  auto *codegen = GetCodeGen();
  tuple_count_ = pipeline->DeclarePipelineStateEntry("numTuples", codegen->Int32Type());
}

void LimitTranslator::InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();
  function->Append(codegen->Assign(tuple_count_.Get(codegen), codegen->Const64(0)));
}

void LimitTranslator::PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const {
  const auto &plan = GetPlanAs<planner::LimitPlanNode>();
  auto *codegen = GetCodeGen();

  // Build the limit/offset condition check:
  // if (numTuples >= plan.offset and numTuples < plan.limit)
  ast::Expr *cond = nullptr;
  if (plan.GetOffset() != 0) {
    cond = codegen->Compare(parsing::Token::Type::GREATER_EQUAL, tuple_count_.Get(codegen),
                            codegen->Const32(plan.GetOffset()));
  }
  if (plan.GetLimit() != 0) {
    auto limit_check = codegen->Compare(parsing::Token::Type::LESS, tuple_count_.Get(codegen),
                                        codegen->Const32(plan.GetOffset() + plan.GetLimit()));
    cond = cond == nullptr ? limit_check : codegen->BinaryOp(parsing::Token::Type::AND, cond, limit_check);
  }

  If check_limit(function, cond);
  context->Push(function);
  check_limit.EndIf();

  // Update running count: numTuples += 1
  auto increment = codegen->BinaryOp(parsing::Token::Type::PLUS, tuple_count_.Get(codegen), codegen->Const32(1));
  function->Append(codegen->Assign(tuple_count_.Get(codegen), increment));
}

}  // namespace noisepage::execution::compiler
