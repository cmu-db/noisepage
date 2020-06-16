#include "execution/codegen/operators/output_translator.h"

#include "execution/codegen/codegen.h"
#include "execution/codegen/compilation_context.h"
#include "execution/codegen/function_builder.h"
#include "execution/codegen/if.h"
#include "execution/codegen/loop.h"
#include "planner/plannodes/aggregate_plan_node.h"

namespace terrier::execution::codegen {

namespace {
constexpr char kOutputColPrefix[] = "out";
}  // namespace

OutputTranslator::OutputTranslator(const planner::AbstractPlanNode &plan, CompilationContext *compilation_context,
                                   Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline, brain::ExecutionOperatingUnitType::OUTPUT),
      output_var_(GetCodeGen()->MakeFreshIdentifier("outRow")),
      output_struct_(GetCodeGen()->MakeFreshIdentifier("OutputStruct")) {
  // Prepare the child.
  compilation_context->Prepare(plan, pipeline);
}

void OutputTranslator::PerformPipelineWork(terrier::execution::codegen::WorkContext *context,
                                           terrier::execution::codegen::FunctionBuilder *function) const {
  // First generate the call @resultBufferAllocRow(execCtx)
  auto exec_ctx = GetExecutionContext();
  ast::Expr *alloc_call = GetCodeGen()->CallBuiltin(ast::Builtin::ResultBufferAllocOutRow, {exec_ctx});
  ast::Expr *cast_call = GetCodeGen()->PtrCast(output_struct_, alloc_call);
  function->Append(GetCodeGen()->DeclareVar(output_var_, nullptr, cast_call));
  const auto child_translator = GetCompilationContext()->LookupTranslator(GetPlan());

  // Now fill up the output row
  // For each column in the output, set out.col_i = col_i
  for (uint32_t attr_idx = 0; attr_idx < GetPlan().GetOutputSchema()->NumColumns(); attr_idx++) {
    ast::Identifier attr_name = GetCodeGen()->MakeIdentifier(kOutputColPrefix + std::to_string(attr_idx));
    ast::Expr *lhs = GetCodeGen()->AccessStructMember(GetCodeGen()->MakeExpr(output_var_), attr_name);
    ast::Expr *rhs = child_translator->GetOutput(context, attr_idx);
    function->Append(GetCodeGen()->Assign(lhs, rhs));
  }
}

void OutputTranslator::FinishPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const {
  auto exec_ctx = GetExecutionContext();
  function->Append(GetCodeGen()->CallBuiltin(ast::Builtin::ResultBufferFinalize, {exec_ctx}));
}

void OutputTranslator::DefineHelperStructs(util::RegionVector<ast::StructDecl *> *decls) {
  CodeGen *codegen = GetCodeGen();
  auto fields = codegen->MakeEmptyFieldList();

  const auto output_schema = GetPlan().GetOutputSchema();
  fields.reserve(output_schema->NumColumns());

  // Add columns to output.
  uint32_t attr_idx = 0;
  for (const auto &col : output_schema->GetColumns()) {
    auto field_name = codegen->MakeIdentifier(kOutputColPrefix + std::to_string(attr_idx++));
    auto type = codegen->TplType(sql::GetTypeId(col.GetExpr()->GetReturnValueType()));
    fields.emplace_back(codegen->MakeField(field_name, type));
  }

  decls->push_back(codegen->DeclareStruct(output_struct_, std::move(fields)));
}

}  // namespace terrier::execution::codegen
