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

namespace noisepage::execution::compiler {

IndexJoinTranslator::IndexJoinTranslator(const planner::IndexJoinPlanNode &plan,
                                         CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline, brain::ExecutionOperatingUnitType::DUMMY),
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
  index_size_ = CounterDeclare("index_size", pipeline);
  num_scans_index_ = CounterDeclare("num_scans_index", pipeline);
  num_loops_ = CounterDeclare("num_loops", pipeline);
}

void IndexJoinTranslator::InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const {
  CounterSet(function, index_size_, 0);
  CounterSet(function, num_scans_index_, 0);
  CounterSet(function, num_loops_, 0);
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

  CounterAdd(function, num_loops_, 1);

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

    CounterAdd(function, num_scans_index_, 1);
  }
  loop.EndLoop();

  CounterSetExpr(function, index_size_,
                 GetCodeGen()->CallBuiltin(ast::Builtin::IndexIteratorGetSize, {GetCodeGen()->AddressOf(index_iter_)}));
  // @indexIteratorFree(&index_iter_)
  FreeIterator(function);
}

void IndexJoinTranslator::FinishPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const {
  // To match the models, IDX_SCAN::CARDINALITY is recorded as per-loop num scans.
  // i.e. if num loops > 0, this is recorded as int(num_scans_index_ / num_loops_)
  if (IsCountersEnabled()) {
    auto *codegen = GetCodeGen();
    // var per_loop_num_scans = queryState.num_scans_index
    // if (queryState.num_loops > 0) { per_loop_num_scans = per_loop_num_scans / queryState.num_loops }
    ast::Identifier per_loop_num_scans = codegen->MakeFreshIdentifier("per_loop_num_scans");
    ast::Expr *per_loop_num_scans_expr = codegen->MakeExpr(per_loop_num_scans);
    function->Append(codegen->DeclareVarWithInit(per_loop_num_scans, CounterVal(num_scans_index_)));
    auto *num_loops_pos = codegen->Compare(parsing::Token::Type::GREATER, CounterVal(num_loops_), codegen->Const32(0));
    If check(function, num_loops_pos);
    {
      ast::Expr *div = codegen->BinaryOp(parsing::Token::Type::SLASH, per_loop_num_scans_expr, CounterVal(num_loops_));
      function->Append(codegen->Assign(per_loop_num_scans_expr, div));
    }
    check.EndIf();

    FeatureRecord(function, brain::ExecutionOperatingUnitType::IDX_SCAN,
                  brain::ExecutionOperatingUnitFeatureAttribute::CARDINALITY, pipeline, per_loop_num_scans_expr);
  }

  FeatureRecord(function, brain::ExecutionOperatingUnitType::IDX_SCAN,
                brain::ExecutionOperatingUnitFeatureAttribute::NUM_ROWS, pipeline, CounterVal(index_size_));
  FeatureRecord(function, brain::ExecutionOperatingUnitType::IDX_SCAN,
                brain::ExecutionOperatingUnitFeatureAttribute::NUM_LOOPS, pipeline, CounterVal(num_loops_));
  FeatureArithmeticRecordMul(function, pipeline, GetTranslatorId(), CounterVal(num_scans_index_));
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
    ast::Expr *rhs = GetCodeGen()->Const32(input_oids_[i].UnderlyingValue());
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

  ast::Expr *init_call = GetCodeGen()->IndexIteratorInit(
      index_iter_, GetCompilationContext()->GetExecutionContextPtrFromQueryState(), num_attrs,
      op.GetTableOid().UnderlyingValue(), op.GetIndexOid().UnderlyingValue(), col_oids_);
  builder->Append(GetCodeGen()->MakeStmt(init_call));
}

void IndexJoinTranslator::DeclareIndexPR(noisepage::execution::compiler::FunctionBuilder *builder) const {
  // var lo_pr = @indexIteratorGetLoPR(&index_iter)
  // var hi_pr = @indexIteratorGetHiPR(&index_iter)
  ast::Expr *lo_pr_call =
      GetCodeGen()->CallBuiltin(ast::Builtin::IndexIteratorGetLoPR, {GetCodeGen()->AddressOf(index_iter_)});
  ast::Expr *hi_pr_call =
      GetCodeGen()->CallBuiltin(ast::Builtin::IndexIteratorGetHiPR, {GetCodeGen()->AddressOf(index_iter_)});
  builder->Append(GetCodeGen()->DeclareVar(lo_index_pr_, nullptr, lo_pr_call));
  builder->Append(GetCodeGen()->DeclareVar(hi_index_pr_, nullptr, hi_pr_call));
}

void IndexJoinTranslator::DeclareTablePR(noisepage::execution::compiler::FunctionBuilder *builder) const {
  // var table_pr = @indexIteratorGetTablePR(&index_iter)
  ast::Expr *get_pr_call =
      GetCodeGen()->CallBuiltin(ast::Builtin::IndexIteratorGetTablePR, {GetCodeGen()->AddressOf(index_iter_)});
  builder->Append(GetCodeGen()->DeclareVar(table_pr_, nullptr, get_pr_call));
}

void IndexJoinTranslator::DeclareSlot(noisepage::execution::compiler::FunctionBuilder *builder) const {
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
    type::TypeId attr_type = index_schema_.GetColumn(key.first.UnderlyingValue() - 1).Type();
    bool nullable = index_schema_.GetColumn(key.first.UnderlyingValue() - 1).Nullable();
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

}  // namespace noisepage::execution::compiler
