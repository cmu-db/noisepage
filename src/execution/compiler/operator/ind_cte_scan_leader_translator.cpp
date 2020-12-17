#include "execution/compiler/operator/ind_cte_scan_leader_translator.h"

#include <execution/compiler/loop.h>

#include "execution/compiler/codegen.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/work_context.h"
#include "parser/expression/constant_value_expression.h"

namespace noisepage::execution::compiler {

IndCteScanLeaderTranslator::IndCteScanLeaderTranslator(const planner::CteScanPlanNode &plan,
                                                       CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline, selfdriving::ExecutionOperatingUnitType::CTE_SCAN),
      col_types_(GetCodeGen()->MakeFreshIdentifier("col_types")),
      col_oids_var_(GetCodeGen()->MakeFreshIdentifier("col_oids")),
      insert_pr_(GetCodeGen()->MakeFreshIdentifier("insert_pr")),
      base_pipeline_(this, Pipeline::Parallelism::Serial),
      build_pipeline_(this, Pipeline::Parallelism::Serial) {
  auto iter_cte_type = GetCodeGen()->BuiltinType(ast::BuiltinType::Kind::IndCteScanIterator);
  auto cte_type = GetCodeGen()->BuiltinType(ast::BuiltinType::Kind::CteScanIterator);
  cte_scan_val_entry_ = compilation_context->GetQueryState()->DeclareStateEntry(
      GetCodeGen(), plan.GetCTETableName() + "val", iter_cte_type);
  cte_scan_ptr_entry_ = compilation_context->GetQueryState()->DeclareStateEntry(
      GetCodeGen(), plan.GetCTETableName() + "ptr", GetCodeGen()->PointerType(cte_type));

  pipeline->LinkNestedPipeline(&build_pipeline_);
  pipeline->LinkNestedPipeline(&base_pipeline_);
  pipeline->UpdateParallelism(Pipeline::Parallelism::Serial);
  compilation_context->Prepare(*(plan.GetChild(1)), &base_pipeline_);
  compilation_context->Prepare(*(plan.GetChild(0)), &build_pipeline_);
}

void IndCteScanLeaderTranslator::TearDownQueryState(FunctionBuilder *function) const {
  ast::Expr *cte_free_call =
      GetCodeGen()->CallBuiltin(ast::Builtin::CteScanFree, {cte_scan_ptr_entry_.Get(GetCodeGen())});
  function->Append(GetCodeGen()->MakeStmt(cte_free_call));
}

void IndCteScanLeaderTranslator::PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const {
  if (&context->GetPipeline() == &base_pipeline_) {
    DeclareInsertPR(function);
    GetInsertPR(function);
    FillPRFromChild(context, function, 1);
    GenTableInsert(function);
    PopulateReadCteScanIterator(function);
    return;
  }

  if (&context->GetPipeline() != &build_pipeline_) {
    auto calls = base_pipeline_.CallRunPipelineFunction();
    for (auto call : calls) {
      function->Append(call);
    }
    GenInductiveLoop(context, function);
    context->Push(function);
    return;
  }

  // Declare & Get table PR
  DeclareInsertPR(function);
  GetInsertPR(function);

  // Set the values to insert
  FillPRFromChild(context, function, 0);

  // Insert into table
  GenTableInsert(function);
  context->Push(function);
}

void IndCteScanLeaderTranslator::DeclareIndCteScanIterator(FunctionBuilder *builder) const {
  // Generate col types
  auto codegen = GetCodeGen();
  SetColumnTypes(builder);
  SetColumnOids(builder);
  auto &plan = GetPlanAs<planner::CteScanPlanNode>();
  // Call @cteScanIteratorInit
  ast::Expr *cte_scan_iterator_setup = codegen->IndCteScanIteratorInit(
      cte_scan_val_entry_.GetPtr(codegen), plan.GetTableOid(), col_oids_var_, col_types_, plan.GetIsRecursive(),
      GetCompilationContext()->GetExecutionContextPtrFromQueryState());
  builder->Append(codegen->MakeStmt(cte_scan_iterator_setup));

  ast::Stmt *pointer_setup =
      codegen->Assign(cte_scan_ptr_entry_.Get(codegen),
                      codegen->CallBuiltin(ast::Builtin::IndCteScanGetReadCte, {cte_scan_val_entry_.GetPtr(codegen)}));
  builder->Append(pointer_setup);
}

void IndCteScanLeaderTranslator::SetColumnTypes(FunctionBuilder *builder) const {
  // Declare: var col_types: [num_cols]uint32
  auto &plan = GetPlanAs<planner::CteScanPlanNode>();
  auto codegen = GetCodeGen();
  auto size = plan.GetTableSchema()->GetColumns().size();
  ast::Expr *arr_type = codegen->ArrayType(size, ast::BuiltinType::Kind::Uint32);
  builder->Append(codegen->DeclareVar(col_types_, arr_type, nullptr));

  // For each oid, set col_oids[i] = col_oid
  for (uint16_t i = 0; i < size; i++) {
    ast::Expr *lhs = codegen->ArrayAccess(col_types_, i);
    ast::Expr *rhs = codegen->Const32(static_cast<uint32_t>(plan.GetTableSchema()->GetColumns()[i].Type()));
    builder->Append(codegen->Assign(lhs, rhs));
  }
}

void IndCteScanLeaderTranslator::SetColumnOids(FunctionBuilder *builder) const {
  // Declare: var col_types: [num_cols]uint32
  auto codegen = GetCodeGen();
  auto &plan = GetPlanAs<planner::CteScanPlanNode>();
  auto size = plan.GetTableSchema()->GetColumns().size();
  ast::Expr *arr_type = codegen->ArrayType(size, ast::BuiltinType::Kind::Uint32);
  builder->Append(codegen->DeclareVar(col_oids_var_, arr_type, nullptr));

  // For each oid, set col_oids[i] = col_oid
  for (uint16_t i = 0; i < size; i++) {
    ast::Expr *lhs = codegen->ArrayAccess(col_oids_var_, i);
    ast::Expr *rhs = codegen->Const32(static_cast<uint32_t>(plan.GetTableSchema()->GetColumns()[i].Oid()));
    builder->Append(codegen->Assign(lhs, rhs));
  }
}

void IndCteScanLeaderTranslator::InitializeQueryState(FunctionBuilder *function) const {
  DeclareIndCteScanIterator(function);
}

ast::Expr *IndCteScanLeaderTranslator::GetCteScanPtr(CodeGen *codegen) const {
  return cte_scan_ptr_entry_.Get(codegen);
}

void IndCteScanLeaderTranslator::DeclareInsertPR(noisepage::execution::compiler::FunctionBuilder *builder) const {
  // var insert_pr : *ProjectedRow
  auto codegen = GetCodeGen();
  auto pr_type = codegen->BuiltinType(ast::BuiltinType::Kind::ProjectedRow);
  builder->Append(codegen->DeclareVar(insert_pr_, codegen->PointerType(pr_type), nullptr));
}

void IndCteScanLeaderTranslator::GetInsertPR(noisepage::execution::compiler::FunctionBuilder *builder) const {
  // var insert_pr = cteScanGetInsertTempTablePR(...)
  auto codegen = GetCodeGen();
  auto get_pr_call =
      codegen->CallBuiltin(ast::Builtin::IndCteScanGetInsertTempTablePR, {cte_scan_val_entry_.GetPtr(codegen)});
  builder->Append(codegen->Assign(codegen->MakeExpr(insert_pr_), get_pr_call));
}

void IndCteScanLeaderTranslator::GenTableInsert(FunctionBuilder *builder) const {
  // var insert_slot = @cteScanTableInsert(&inserter_)
  auto codegen = GetCodeGen();
  auto insert_slot = codegen->MakeFreshIdentifier("insert_slot");
  auto insert_call = codegen->CallBuiltin(ast::Builtin::IndCteScanTableInsert, {cte_scan_val_entry_.GetPtr(codegen)});
  builder->Append(codegen->DeclareVar(insert_slot, nullptr, insert_call));
}

void IndCteScanLeaderTranslator::FillPRFromChild(WorkContext *context, FunctionBuilder *builder,
                                                 uint32_t child_idx) const {
  auto &plan = GetPlanAs<planner::CteScanPlanNode>();
  const auto &cols = plan.GetTableSchema()->GetColumns();
  auto codegen = GetCodeGen();

  for (const auto &col : cols) {
    const auto &table_col_oid = col.Oid();
    size_t col_ind = 0;
    for (auto col_oid : plan.GetColumnOids()) {
      if (col_oid == table_col_oid) {
        break;
      }
      col_ind++;
    }
    auto val = GetChildOutput(context, child_idx, col_ind);
    // TODO(Rohan): Figure how to get the general schema of a child node in case the field is Nullable
    // Right now it is only Non Null
    auto pr_set_call = codegen->PRSet(codegen->MakeExpr(insert_pr_), col.Type(), false, col_ind, val, true);
    builder->Append(codegen->MakeStmt(pr_set_call));
  }
}

void IndCteScanLeaderTranslator::FinalizeReadCteScanIterator(FunctionBuilder *builder) const {
  ast::Expr *cte_scan_iterator_setup =
      GetCodeGen()->CallBuiltin(ast::Builtin::IndCteScanGetResult, {cte_scan_val_entry_.GetPtr(GetCodeGen())});
  ast::Stmt *assign = GetCodeGen()->Assign(cte_scan_ptr_entry_.Get(GetCodeGen()), cte_scan_iterator_setup);
  builder->Append(assign);
}

void IndCteScanLeaderTranslator::GenInductiveLoop(WorkContext *context, FunctionBuilder *builder) const {
  Loop loop(builder, nullptr,
            GetCodeGen()->CallBuiltin(ast::Builtin::IndCteScanAccumulate, {cte_scan_val_entry_.GetPtr(GetCodeGen())}),
            nullptr);
  {
    PopulateReadCteScanIterator(builder);
    auto calls = build_pipeline_.CallRunPipelineFunction();
    for (auto call : calls) {
      builder->Append(call);
    }
  }
  loop.EndLoop();

  FinalizeReadCteScanIterator(builder);
}

void IndCteScanLeaderTranslator::PopulateReadCteScanIterator(FunctionBuilder *builder) const {
  ast::Expr *cte_scan_iterator_setup =
      GetCodeGen()->CallBuiltin(ast::Builtin::IndCteScanGetReadCte, {cte_scan_val_entry_.GetPtr(GetCodeGen())});
  ast::Stmt *assign = GetCodeGen()->Assign(cte_scan_ptr_entry_.Get(GetCodeGen()), cte_scan_iterator_setup);
  builder->Append(assign);
}

}  // namespace noisepage::execution::compiler
