#include "execution/compiler/operator/cte_scan_translator.h"

#include "execution/compiler/codegen.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/if.h"
#include "execution/compiler/loop.h"
#include "execution/compiler/operator/cte_scan_leader_translator.h"
#include "execution/compiler/work_context.h"
#include "parser/expression/constant_value_expression.h"

namespace terrier::execution::compiler {

parser::ConstantValueExpression DummyCVE() {
  return terrier::parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(0));
}

CteScanTranslator::CteScanTranslator(const planner::CteScanPlanNode &plan, CompilationContext *compilation_context,
                                     Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline, brain::ExecutionOperatingUnitType::CTE_SCAN),
      op_(&plan),
      col_types_(GetCodeGen()->MakeFreshIdentifier("col_types")),
      insert_pr_(GetCodeGen()->MakeFreshIdentifier("insert_pr")),
      read_col_oids_(GetCodeGen()->MakeFreshIdentifier("read_col_oids")),
      read_tvi_(GetCodeGen()->MakeFreshIdentifier("temp_table_iterator")),
      read_vpi_(GetCodeGen()->MakeFreshIdentifier("read_pci")),
      read_slot_(GetCodeGen()->MakeFreshIdentifier("read_slot")) {
  // ToDo(Gautam,Preetansh): Send the complete schema in the plan node.

  if(plan.GetChildrenSize() > 0){
    compilation_context->Prepare(*(plan.GetChild(0)), pipeline);
  }
  if(plan.GetScanPredicate() != nullptr) {
    compilation_context->Prepare(*plan.GetScanPredicate());
  }
}

ast::Expr *CteScanTranslator::GetCteScanIterator() const {
  auto leader_translator = reinterpret_cast<CteScanLeaderTranslator*>(
      GetCompilationContext()->LookupTranslator(*op_->GetLeader()));
  return leader_translator->GetCteScanPtr(GetCodeGen());
}


void CteScanTranslator::SetReadOids(FunctionBuilder *builder) const {
  // Declare: var col_oids: [num_cols]uint32
  auto columns = op_->GetTableSchema()->GetColumns();
  ast::Expr *arr_type = GetCodeGen()->ArrayType(columns.size(), ast::BuiltinType::Kind::Uint32);
  builder->Append(GetCodeGen()->DeclareVar(read_col_oids_, arr_type, nullptr));

  // For each oid, set col_oids[i] = col_oid
  for (uint16_t i = 0; i < columns.size(); i++) {
    ast::Expr *lhs = GetCodeGen()->ArrayAccess(read_col_oids_, i);
    ast::Expr *rhs = GetCodeGen()->Const32(!columns[i].Oid());
    builder->Append(GetCodeGen()->Assign(lhs, rhs));
  }
}

void CteScanTranslator::DeclareReadTVI(FunctionBuilder *builder) const {
  // var tvi: TableVectorIterator
  ast::Expr *iter_type = GetCodeGen()->BuiltinType(ast::BuiltinType::Kind::TableVectorIterator);
  builder->Append(GetCodeGen()->DeclareVar(read_tvi_, iter_type, nullptr));

  // Call @tableIterInit(&tvi, execCtx, table_oid, col_oids)
  ast::Expr *init_call = GetCodeGen()->TempTableIterInit(read_tvi_,
                                                    GetCteScanIterator(),
                                                     read_col_oids_,
                                                         GetCompilationContext()->GetExecutionContextPtrFromQueryState());
  builder->Append(GetCodeGen()->MakeStmt(init_call));

  builder->Append(GetCodeGen()->DeclareVarNoInit(read_slot_, GetCodeGen()->BuiltinType(ast::BuiltinType::TupleSlot)));
}
void CteScanTranslator::GenReadTVIClose(FunctionBuilder *builder) const {
  // Close iterator
  ast::Expr *close_call = GetCodeGen()->CallBuiltin(ast::Builtin::TableIterClose, {GetCodeGen()->AddressOf(read_tvi_)});
  builder->Append(GetCodeGen()->MakeStmt(close_call));
}

void CteScanTranslator::DoTableScan(WorkContext *context, FunctionBuilder *builder) const {
  // Start looping over the table
  auto codegen = GetCodeGen();
  ast::Expr *advance_call = codegen->CallBuiltin(ast::Builtin::TableIterAdvance,
                                                      {codegen->AddressOf(read_tvi_)});
  Loop tvi_loop(builder, nullptr, advance_call, nullptr);
  {
    auto vpi = codegen->MakeExpr(read_vpi_);
    builder->Append(codegen->DeclareVarWithInit(read_vpi_,
                                                codegen->TableIterGetVPI(codegen->AddressOf(read_tvi_))));
    Loop vpi_loop(builder, nullptr, codegen->VPIHasNext(vpi, false),
                  codegen->MakeStmt(codegen->VPIAdvance(vpi, false)));
    {
      if (op_->GetScanPredicate() != nullptr) {
        ast::Expr *cond = context->DeriveValue(*op_->GetScanPredicate(), this);

        If predicate(builder, cond);
        {
          // Declare Slot.
          DeclareSlot(builder);
          // Let parent consume.
          context->Push(builder);
        }
        predicate.EndIf();
      } else {
        DeclareSlot(builder);
        context->Push(builder);
      }
    }
    vpi_loop.EndLoop();
  }
  tvi_loop.EndLoop();
  GenReadTVIClose(builder);
}

void CteScanTranslator::DeclareSlot(FunctionBuilder *builder) const {
  // var slot = @tableIterGetSlot(vpi)
  auto codegen = GetCodeGen();
  auto make_slot = codegen->CallBuiltin(ast::Builtin::VPIGetSlot, {codegen->MakeExpr(read_vpi_)});
  auto assign = codegen->Assign(codegen->MakeExpr(read_slot_), make_slot);
  builder->Append(assign);
}

//void CteScanTranslator::GenReadTVIReset(FunctionBuilder *builder) {
//  ast::Expr *reset_call = GetCodeGen()->CallBuiltin(ast::Builtin::TableIterReset, read_tvi_, true);
//  builder->Append(GetCodeGen()->MakeStmt(reset_call));
//}
void CteScanTranslator::TearDownPipelineState(const Pipeline &pipeline, FunctionBuilder *function) const {
  OperatorTranslator::TearDownPipelineState(pipeline, function);
}
void CteScanTranslator::InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const {
  OperatorTranslator::TearDownPipelineState(pipeline, function);
}
void CteScanTranslator::PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const {
  SetReadOids(function);
  DeclareReadTVI(function);
  DoTableScan(context, function);
}
ast::Expr *CteScanTranslator::GetTableColumn(catalog::col_oid_t col_oid) const {
  const auto &schema = op_->GetTableSchema();
  auto type = schema->GetColumn(col_oid).Type();
  auto nullable = schema->GetColumn(col_oid).Nullable();
  auto col_index = EXTRACT_OID(catalog::col_oid_t, col_oid);
  return GetCodeGen()->VPIGet(GetCodeGen()->MakeExpr(read_vpi_), sql::GetTypeId(type), nullable, !col_index);;
}

ast::Expr *CteScanTranslator::GetSlotAddress() const { return GetCodeGen()->AddressOf(read_slot_); }

}  // namespace terrier::execution::compiler
