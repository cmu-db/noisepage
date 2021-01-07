#include "execution/compiler/operator/cte_scan_leader_translator.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/work_context.h"
#include "parser/expression/constant_value_expression.h"

namespace noisepage::execution::compiler {

CteScanLeaderTranslator::CteScanLeaderTranslator(const planner::CteScanPlanNode &plan,
                                                 CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline, selfdriving::ExecutionOperatingUnitType::CTE_SCAN),
      col_oids_var_(GetCodeGen()->MakeFreshIdentifier("col_oids")),
      col_types_(GetCodeGen()->MakeFreshIdentifier("col_types")),
      insert_pr_(GetCodeGen()->MakeFreshIdentifier("insert_pr")),
      build_pipeline_(this, Pipeline::Parallelism::Serial) {
  auto cte_type = GetCodeGen()->BuiltinType(ast::BuiltinType::Kind::CteScanIterator);
  cte_scan_val_entry_ =
      compilation_context->GetQueryState()->DeclareStateEntry(GetCodeGen(), plan.GetCTETableName() + "val", cte_type);
  cte_scan_ptr_entry_ = compilation_context->GetQueryState()->DeclareStateEntry(
      GetCodeGen(), plan.GetCTETableName() + "ptr", GetCodeGen()->PointerType(cte_type));

  pipeline->LinkSourcePipeline(&build_pipeline_);
  pipeline->UpdateParallelism(Pipeline::Parallelism::Serial);
  compilation_context->Prepare(*(plan.GetChild(0)), &build_pipeline_);

  std::vector<uint16_t> attr_sizes;
  for (const auto &column : plan.GetTableSchema()->GetColumns()) {
    attr_sizes.push_back(column.AttributeLength());
  }

  auto offsets = storage::StorageUtil::ComputeBaseAttributeOffsets(attr_sizes, 0);

  // Build the map from Schema columns to underlying columns
  storage::StorageUtil::PopulateColumnMap(&table_pm_, plan.GetTableSchema()->GetColumns(), &offsets);
}

void CteScanLeaderTranslator::TearDownQueryState(FunctionBuilder *function) const {
  ast::Expr *cte_free_call =
      GetCodeGen()->CallBuiltin(ast::Builtin::CteScanFree, {cte_scan_ptr_entry_.Get(GetCodeGen())});
  function->Append(GetCodeGen()->MakeStmt(cte_free_call));
}

void CteScanLeaderTranslator::PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const {
  if (&context->GetPipeline() != &build_pipeline_) {
    context->Push(function);
    return;
  }

  // Declare & Get table PR
  DeclareInsertPR(function);
  GetInsertPR(function);

  // Set the values to insert
  FillPRFromChild(context, function);

  // Insert into table
  GenTableInsert(function);
}

void CteScanLeaderTranslator::DeclareCteScanIterator(FunctionBuilder *builder) const {
  // Generate col types
  auto codegen = GetCodeGen();
  SetColumnTypes(builder);
  SetColumnOids(builder);
  // Call @cteScanIteratorInit
  ast::Expr *cte_scan_iterator_setup = codegen->CteScanIteratorInit(
      cte_scan_val_entry_.GetPtr(codegen), GetPlanAs<planner::CteScanPlanNode>().GetTableOid(), col_oids_var_,
      col_types_, GetCompilationContext()->GetExecutionContextPtrFromQueryState());
  builder->Append(codegen->MakeStmt(cte_scan_iterator_setup));

  ast::Stmt *pointer_setup = codegen->Assign(cte_scan_ptr_entry_.Get(codegen), cte_scan_val_entry_.GetPtr(codegen));
  builder->Append(pointer_setup);
}

void CteScanLeaderTranslator::SetColumnTypes(FunctionBuilder *builder) const {
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

void CteScanLeaderTranslator::SetColumnOids(FunctionBuilder *builder) const {
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

void CteScanLeaderTranslator::InitializeQueryState(FunctionBuilder *function) const {
  DeclareCteScanIterator(function);
}

ast::Expr *CteScanLeaderTranslator::GetCteScanPtr(CodeGen *codegen) const { return cte_scan_ptr_entry_.Get(codegen); }

void CteScanLeaderTranslator::DeclareInsertPR(noisepage::execution::compiler::FunctionBuilder *builder) const {
  // var insert_pr : *ProjectedRow
  auto codegen = GetCodeGen();
  auto pr_type = codegen->BuiltinType(ast::BuiltinType::Kind::ProjectedRow);
  builder->Append(codegen->DeclareVar(insert_pr_, codegen->PointerType(pr_type), nullptr));
}

void CteScanLeaderTranslator::GetInsertPR(noisepage::execution::compiler::FunctionBuilder *builder) const {
  // var insert_pr = @cteScanGetInsertTempTablePR(...)
  auto codegen = GetCodeGen();
  auto get_pr_call = codegen->CallBuiltin(ast::Builtin::CteScanGetInsertTempTablePR, {GetCteScanPtr(codegen)});
  builder->Append(codegen->Assign(codegen->MakeExpr(insert_pr_), get_pr_call));
}

void CteScanLeaderTranslator::GenTableInsert(FunctionBuilder *builder) const {
  // var insert_slot = @cteScanTableInsert(&inserter_)
  auto codegen = GetCodeGen();
  auto insert_slot = codegen->MakeFreshIdentifier("insert_slot");
  auto insert_call = codegen->CallBuiltin(ast::Builtin::CteScanTableInsert, {GetCteScanPtr(codegen)});
  builder->Append(codegen->DeclareVar(insert_slot, nullptr, insert_call));
}

void CteScanLeaderTranslator::FillPRFromChild(WorkContext *context, FunctionBuilder *builder) const {
  auto &plan = GetPlanAs<planner::CteScanPlanNode>();
  const auto &cols = plan.GetTableSchema()->GetColumns();
  auto codegen = GetCodeGen();

  for (const auto &col : cols) {
    const auto &table_col_oid = col.Oid();
    size_t col_ind = 0;
    for (auto col_id : plan.GetColumnOids()) {
      if (col_id == table_col_oid) {
        break;
      }
      col_ind++;
    }
    auto val = GetChildOutput(context, 0, col_ind);
    // TODO(Rohan): Figure how to get the general schema of a child node in case the field is Nullable
    // Right now it is only Non Null
    auto insertion_val = codegen->MakeFreshIdentifier("set-val");
    auto set_decl = codegen->DeclareVar(insertion_val, codegen->TplType(execution::sql::GetTypeId(col.Type())), val);
    builder->Append(set_decl);

    auto pr_set_call = codegen->PRSet(codegen->MakeExpr(insert_pr_), col.Type(), false,
                                      table_pm_.find(table_col_oid)->second.col_id_.UnderlyingValue(),
                                      codegen->MakeExpr(insertion_val), true);
    builder->Append(codegen->MakeStmt(pr_set_call));
  }
}
}  // namespace noisepage::execution::compiler
