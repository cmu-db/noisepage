#include "execution/compiler/operator/output_translator.h"

#include "brain/operating_unit_util.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/if.h"
#include "execution/compiler/loop.h"
#include "planner/plannodes/aggregate_plan_node.h"

namespace terrier::execution::compiler {

namespace {
constexpr char OUTPUT_COL_PREFIX[] = "out";
}  // namespace

OutputTranslator::OutputTranslator(const planner::AbstractPlanNode &plan, CompilationContext *compilation_context,
                                   Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline, brain::ExecutionOperatingUnitType::OUTPUT),
      output_var_(GetCodeGen()->MakeFreshIdentifier("outRow")),
      output_struct_(GetCodeGen()->MakeFreshIdentifier("OutputStruct")) {
  // Prepare the child.
  compilation_context->Prepare(plan, pipeline);

  if (IsCountersEnabled()) {
    auto *codegen = GetCodeGen();
    ast::Expr *num_output_type = codegen->BuiltinType(ast::BuiltinType::Uint32);
    num_output_ = compilation_context->GetQueryState()->DeclareStateEntry(codegen, "num_output", num_output_type);
  }
}

void OutputTranslator::InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const {
  if (IsCountersEnabled()) {
    // queryState.num_output = 0
    auto *codegen = GetCodeGen();
    auto assignment = codegen->Assign(num_output_.Get(codegen), codegen->Const32(0));
    function->Append(assignment);
  }
}

void OutputTranslator::PerformPipelineWork(terrier::execution::compiler::WorkContext *context,
                                           terrier::execution::compiler::FunctionBuilder *function) const {
  // First generate the call @resultBufferAllocRow(execCtx)
  auto exec_ctx = GetExecutionContext();
  ast::Expr *alloc_call = GetCodeGen()->CallBuiltin(ast::Builtin::ResultBufferAllocOutRow, {exec_ctx});
  ast::Expr *cast_call = GetCodeGen()->PtrCast(output_struct_, alloc_call);
  function->Append(GetCodeGen()->DeclareVar(output_var_, nullptr, cast_call));
  const auto child_translator = GetCompilationContext()->LookupTranslator(GetPlan());

  // Now fill up the output row
  // For each column in the output, set out.col_i = col_i
  for (uint32_t attr_idx = 0; attr_idx < GetPlan().GetOutputSchema()->NumColumns(); attr_idx++) {
    ast::Identifier attr_name = GetCodeGen()->MakeIdentifier(OUTPUT_COL_PREFIX + std::to_string(attr_idx));
    ast::Expr *lhs = GetCodeGen()->AccessStructMember(GetCodeGen()->MakeExpr(output_var_), attr_name);
    ast::Expr *rhs = child_translator->GetOutput(context, attr_idx);
    function->Append(GetCodeGen()->Assign(lhs, rhs));
  }

  if (IsCountersEnabled()) {
    // queryState.num_output = queryState.num_output + 1
    auto *codegen = GetCodeGen();
    ast::Expr *plus_op = codegen->BinaryOp(parsing::Token::Type::PLUS, num_output_.Get(codegen), codegen->Const32(1));
    ast::Stmt *num_output_increment = codegen->Assign(num_output_.Get(codegen), plus_op);
    function->Append(num_output_increment);
  }
}

void OutputTranslator::FinishPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const {
  auto exec_ctx = GetExecutionContext();
  function->Append(GetCodeGen()->CallBuiltin(ast::Builtin::ResultBufferFinalize, {exec_ctx}));

  if (IsCountersEnabled()) {
    // @execCtxRecordFeature(exec_ctx, pipeline_id, feature_id, NUM_ROWS, queryState.num_output)
    // @execCtxRecordFeature(exec_ctx, pipeline_id, feature_id, CARDINALITY, queryState.num_output)
    auto *codegen = GetCodeGen();
    const pipeline_id_t pipeline_id = pipeline.GetPipelineId();
    const auto &features = this->GetCodeGen()->GetPipelineOperatingUnits()->GetPipelineFeatures(pipeline_id);
    const auto &feature =
        brain::OperatingUnitUtil::GetFeature(GetTranslatorId(), features, brain::ExecutionOperatingUnitType::OUTPUT);
    function->Append(codegen->ExecCtxRecordFeature(GetExecutionContext(), pipeline_id, feature.GetFeatureId(),
                                                   brain::ExecutionOperatingUnitFeatureAttribute::NUM_ROWS,
                                                   num_output_.Get(codegen)));
    function->Append(codegen->ExecCtxRecordFeature(GetExecutionContext(), pipeline_id, feature.GetFeatureId(),
                                                   brain::ExecutionOperatingUnitFeatureAttribute::CARDINALITY,
                                                   codegen->Const32(1)));
  }
}

void OutputTranslator::DefineHelperStructs(util::RegionVector<ast::StructDecl *> *decls) {
  auto *codegen = GetCodeGen();
  auto fields = codegen->MakeEmptyFieldList();

  const auto output_schema = GetPlan().GetOutputSchema();
  fields.reserve(output_schema->NumColumns());

  // Add columns to output.
  uint32_t attr_idx = 0;
  for (const auto &col : output_schema->GetColumns()) {
    auto field_name = codegen->MakeIdentifier(OUTPUT_COL_PREFIX + std::to_string(attr_idx++));
    auto type = codegen->TplType(sql::GetTypeId(col.GetExpr()->GetReturnValueType()));
    fields.emplace_back(codegen->MakeField(field_name, type));
  }

  decls->push_back(codegen->DeclareStruct(output_struct_, std::move(fields)));
}

}  // namespace terrier::execution::compiler
