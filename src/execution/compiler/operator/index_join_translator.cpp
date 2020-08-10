#include "execution/compiler/operator/index_join_translator.h"

#include <unordered_map>

#include "catalog/catalog_accessor.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/if.h"
#include "execution/compiler/loop.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/work_context.h"
#include "planner/plannodes/index_join_plan_node.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"

namespace terrier::execution::compiler {

IndexJoinTranslator::IndexJoinTranslator(const planner::IndexJoinPlanNode &plan,
                                         CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline, brain::ExecutionOperatingUnitType::IDXJOIN),
      input_oids_(plan.CollectInputOids()),
      table_schema_(GetCodeGen()->GetCatalogAccessor()->GetSchema(plan.GetTableOid())),
      table_pm_(GetCodeGen()->GetCatalogAccessor()->GetTable(plan.GetTableOid())->ProjectionMapForOids(input_oids_)),
      index_schema_(GetCodeGen()->GetCatalogAccessor()->GetIndexSchema(plan.GetIndexOid())),
      index_pm_(GetCodeGen()->GetCatalogAccessor()->GetIndex(plan.GetIndexOid())->GetKeyOidToOffsetMap()),
      index_iter_(GetCodeGen()->MakeFreshIdentifier("index_iter")),
      col_oids_(GetCodeGen()->MakeFreshIdentifier("col_oids")),
      lo_index_pr_(GetCodeGen()->MakeFreshIdentifier("lo_index_pr")),
      hi_index_pr_(GetCodeGen()->MakeFreshIdentifier("hi_index_pr")),
      table_pr_(GetCodeGen()->MakeFreshIdentifier("table_pr")),
      slot_(GetCodeGen()->MakeFreshIdentifier("slot")) {
  pipeline->RegisterSource(this, Pipeline::Parallelism::Serial);
  if (plan.GetJoinPredicate() != nullptr) {
    compilation_context->Prepare(*plan.GetJoinPredicate());
  }

  for (const auto &key : plan.GetHiIndexColumns()) {
    compilation_context->Prepare(*key.second);
  }
  for (const auto &key : plan.GetLoIndexColumns()) {
    compilation_context->Prepare(*key.second);
  }

  compilation_context->Prepare(*GetPlan().GetChild(0), pipeline);
}

void IndexJoinTranslator::PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const {
  const auto &op = GetPlanAs<planner::IndexJoinPlanNode>();
  // var col_oids: [num_cols]uint32
  // col_oids[i] = ...
  SetOids(function);
  // var index_iter : IndexIterator
  // @indexIteratorInit(&index_iter, queryState.execCtx, num_attrs, table_oid, index_oid, col_oids)
  DeclareIterator(function);
  // var lo_index_pr = @indexIteratorGetLoPR(&index_iter)
  // var hi_index_pr = @indexIteratorGetHiPR(&index_iter)
  DeclareIndexPR(function);
  // @prSet(lo_index_pr, ...)
  FillKey(context, function, lo_index_pr_, op.GetLoIndexColumns());
  // @prSet(hi_index_pr, ...)
  FillKey(context, function, hi_index_pr_, op.GetHiIndexColumns());

  // @indexIteratorScanKey(&index_iter)
  ast::Expr *scan_call = GetCodeGen()->IndexIteratorScan(index_iter_, op.GetScanType(), 0);
  ast::Stmt *loop_init = GetCodeGen()->MakeStmt(scan_call);
  // @indexIteratorAdvance(&index_iter)
  ast::Expr *advance_call =
      GetCodeGen()->CallBuiltin(ast::Builtin::IndexIteratorAdvance, {GetCodeGen()->AddressOf(index_iter_)});

  // for (@indexIteratorScanKey(&index_iter); @indexIteratorAdvance(&index_iter);)
  Loop loop(function, loop_init, advance_call, nullptr);
  {
    // var table_pr = @indexIteratorGetTablePR(&index_iter)
    DeclareTablePR(function);
    // var slot = @indexIteratorGetSlot(&index_iter)
    DeclareSlot(function);

    bool has_predicate = op.GetJoinPredicate() != nullptr;
    if (has_predicate) {
      ast::Expr *cond = context->DeriveValue(*op.GetJoinPredicate(), this);
      // if (cond) { PARENT_CODE }
      If predicate(function, cond);
      context->Push(function);
      predicate.EndIf();
    } else {
      // PARENT_CODE
      context->Push(function);
    }
  }
  loop.EndLoop();

  // @indexIteratorFree(&index_iter_)
  FreeIterator(function);
}

ast::Expr *IndexJoinTranslator::GetTableColumn(catalog::col_oid_t col_oid) const {
  // @prGet(table_pr, type, nullable, attr_idx)
  auto type = table_schema_.GetColumn(col_oid).Type();
  auto nullable = table_schema_.GetColumn(col_oid).Nullable();
  uint16_t attr_idx = table_pm_.find(col_oid)->second;
  return GetCodeGen()->PRGet(GetCodeGen()->MakeExpr(table_pr_), type, nullable, attr_idx);
}

void IndexJoinTranslator::SetOids(FunctionBuilder *builder) const {
  // var col_oids: [num_cols]uint32
  ast::Expr *arr_type = GetCodeGen()->ArrayType(input_oids_.size(), ast::BuiltinType::Kind::Uint32);
  builder->Append(GetCodeGen()->DeclareVar(col_oids_, arr_type, nullptr));

  for (uint16_t i = 0; i < input_oids_.size(); i++) {
    // col_oids[i] = col_oid
    ast::Expr *lhs = GetCodeGen()->ArrayAccess(col_oids_, i);
    ast::Expr *rhs = GetCodeGen()->Const32(!input_oids_[i]);
    builder->Append(GetCodeGen()->Assign(lhs, rhs));
  }
}

void IndexJoinTranslator::DeclareIterator(FunctionBuilder *builder) const {
  // var index_iter : IndexIterator
  ast::Expr *iter_type = GetCodeGen()->BuiltinType(ast::BuiltinType::IndexIterator);
  builder->Append(GetCodeGen()->DeclareVar(index_iter_, iter_type, nullptr));
  // @indexIteratorInit(&index_iter, queryState.execCtx, num_attrs, table_oid, index_oid, col_oids)
  const auto &op = GetPlanAs<planner::IndexJoinPlanNode>();
  uint32_t num_attrs = std::max(op.GetLoIndexColumns().size(), op.GetHiIndexColumns().size());

  ast::Expr *init_call =
      GetCodeGen()->IndexIteratorInit(index_iter_, GetCompilationContext()->GetExecutionContextPtrFromQueryState(),
                                      num_attrs, !op.GetTableOid(), !op.GetIndexOid(), col_oids_);
  builder->Append(GetCodeGen()->MakeStmt(init_call));
}

void IndexJoinTranslator::DeclareIndexPR(terrier::execution::compiler::FunctionBuilder *builder) const {
  // var lo_pr = @indexIteratorGetLoPR(&index_iter)
  // var hi_pr = @indexIteratorGetHiPR(&index_iter)
  ast::Expr *lo_pr_call =
      GetCodeGen()->CallBuiltin(ast::Builtin::IndexIteratorGetLoPR, {GetCodeGen()->AddressOf(index_iter_)});
  ast::Expr *hi_pr_call =
      GetCodeGen()->CallBuiltin(ast::Builtin::IndexIteratorGetHiPR, {GetCodeGen()->AddressOf(index_iter_)});
  builder->Append(GetCodeGen()->DeclareVar(lo_index_pr_, nullptr, lo_pr_call));
  builder->Append(GetCodeGen()->DeclareVar(hi_index_pr_, nullptr, hi_pr_call));
}

void IndexJoinTranslator::DeclareTablePR(terrier::execution::compiler::FunctionBuilder *builder) const {
  // var table_pr = @indexIteratorGetTablePR(&index_iter)
  ast::Expr *get_pr_call =
      GetCodeGen()->CallBuiltin(ast::Builtin::IndexIteratorGetTablePR, {GetCodeGen()->AddressOf(index_iter_)});
  builder->Append(GetCodeGen()->DeclareVar(table_pr_, nullptr, get_pr_call));
}

void IndexJoinTranslator::DeclareSlot(terrier::execution::compiler::FunctionBuilder *builder) const {
  // var slot = @indexIteratorGetSlot(&index_iter)
  ast::Expr *get_slot_call =
      GetCodeGen()->CallBuiltin(ast::Builtin::IndexIteratorGetSlot, {GetCodeGen()->AddressOf(index_iter_)});
  builder->Append(GetCodeGen()->DeclareVar(slot_, nullptr, get_slot_call));
}

void IndexJoinTranslator::FillKey(
    WorkContext *context, FunctionBuilder *builder, ast::Identifier pr,
    const std::unordered_map<catalog::indexkeycol_oid_t, planner::IndexExpression> &index_exprs) const {
  // For each key attribute,
  for (const auto &key : index_exprs) {
    // @prSet(pr, type, nullable, attr, expr, true)
    uint16_t attr_offset = index_pm_.at(key.first);
    type::TypeId attr_type = index_schema_.GetColumn(!key.first - 1).Type();
    bool nullable = index_schema_.GetColumn(!key.first - 1).Nullable();
    auto *set_key_call = GetCodeGen()->PRSet(GetCodeGen()->MakeExpr(pr), attr_type, nullable, attr_offset,
                                             context->DeriveValue(*key.second.Get(), this), true);
    builder->Append(GetCodeGen()->MakeStmt(set_key_call));
  }
}

ast::Expr *IndexJoinTranslator::GetSlotAddress() const {
  // &slot
  return GetCodeGen()->AddressOf(slot_);
}

void IndexJoinTranslator::FreeIterator(FunctionBuilder *builder) const {
  // @indexIteratorFree(&index_iter_)
  ast::Expr *free_call =
      GetCodeGen()->CallBuiltin(ast::Builtin::IndexIteratorFree, {GetCodeGen()->AddressOf(index_iter_)});
  builder->Append(GetCodeGen()->MakeStmt(free_call));
}

}  // namespace terrier::execution::compiler
