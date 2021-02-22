#include "execution/compiler/operator/analyze_translator.h"

#include "catalog/catalog_accessor.h"
#include "catalog/postgres/builder.h"
#include "catalog/postgres/pg_statistic_impl.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/if.h"
#include "execution/compiler/loop.h"
#include "execution/compiler/pipeline.h"
#include "planner/plannodes/output_schema.h"
#include "spdlog/fmt/fmt.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"

namespace noisepage::execution::compiler {

AnalyzeTranslator::AnalyzeTranslator(const planner::AnalyzePlanNode &plan, CompilationContext *compilation_context,
                                     Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline, selfdriving::ExecutionOperatingUnitType::ANALYZE),
      pg_statistic_table_schema_(catalog::postgres::Builder::GetStatisticTableSchema()),
      pg_statistic_table_pm_(GetCodeGen()
                                 ->GetCatalogAccessor()
                                 ->GetTable(catalog::postgres::PgStatistic::STATISTIC_TABLE_OID)
                                 ->ProjectionMapForOids(std::vector<catalog::col_oid_t>(
                                     catalog::postgres::PgStatistic::PG_STATISTIC_ALL_COL_OIDS.begin(),
                                     catalog::postgres::PgStatistic::PG_STATISTIC_ALL_COL_OIDS.end()))),
      pg_statistic_index_schema_(catalog::postgres::Builder::GetStatisticOidIndexSchema(plan.GetDatabaseOid())),
      pg_statistic_index_pm_(GetCodeGen()
                                 ->GetCatalogAccessor()
                                 ->GetIndex(catalog::postgres::PgStatistic::STATISTIC_OID_INDEX_OID)
                                 ->GetKeyOidToOffsetMap()),
      pg_statistic_col_oids_(GetCodeGen()->MakeFreshIdentifier("pg_statistic_col_oids")),
      table_oid_(GetCodeGen()->MakeFreshIdentifier("table_oid")),
      col_oid_(GetCodeGen()->MakeFreshIdentifier("col_oid")),
      num_rows_(GetCodeGen()->MakeFreshIdentifier("num_rows")),
      pg_statistic_index_iterator_(GetCodeGen()->MakeFreshIdentifier("pg_statistic_index_iterator")),
      pg_statistic_index_pr_(GetCodeGen()->MakeFreshIdentifier("pg_statistic_index_pr")),
      pg_statistic_updater_(GetCodeGen()->MakeFreshIdentifier("pg_statistic_updater")),
      pg_statistic_update_pr_(GetCodeGen()->MakeFreshIdentifier("pg_statistic_update_pr")) {
  // Analyze is serial
  pipeline->RegisterSource(this, Pipeline::Parallelism::Serial);
  // Prepare child
  compilation_context->Prepare(*plan.GetChild(0), pipeline);
  auto *codegen = GetCodeGen();

  pg_statistic_column_lookup_[catalog::postgres::PgStatistic::STARELID.oid_] = table_oid_;
  pg_statistic_column_lookup_[catalog::postgres::PgStatistic::STAATTNUM.oid_] = col_oid_;
  pg_statistic_column_lookup_[catalog::postgres::PgStatistic::STA_NUMROWS.oid_] = num_rows_;

  for (const auto &col_info : catalog::postgres::PgStatisticImpl::ANALYZE_AGGREGATES) {
    auto agg_var = codegen->MakeFreshIdentifier("stat_col_" + std::to_string(col_info.column_oid_.UnderlyingValue()));
    aggregate_variables_.emplace_back(agg_var);
    pg_statistic_column_lookup_[col_info.column_oid_] = agg_var;
  }
}

void AnalyzeTranslator::PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const {
  const auto &plan = GetPlanAs<planner::AnalyzePlanNode>();
  auto *codegen = GetCodeGen();

  NOISEPAGE_ASSERT(plan.GetChild(0)->GetOutputSchema()->NumColumns() ==
                       catalog::postgres::PgStatisticImpl::NUM_ANALYZE_AGGREGATES * plan.GetColumnOids().size() + 1,
                   fmt::format("We should have 1 aggregate for the number of rows and then {} aggregates per column",
                               catalog::postgres::PgStatisticImpl::NUM_ANALYZE_AGGREGATES)
                       .c_str());

  // var pg_statistic_col_oids: [num_cols]uint32
  // pg_statistic_col_oids[i] = ...
  SetPgStatisticColOids(function);

  // Declare/Initialize variables to insert into pg_statistic
  InitPgStatisticVariables(context, function);

  // var pg_statistic_index_iterator : IndexIterator
  DeclarePgStatisticIterator(function);

  // var pg_statistic_index_pr : *ProjectedRow
  DeclarePgStatisticIndexPR(function);

  // Perform a lookup and update for each column
  for (size_t column_offset = 0; column_offset < plan.GetColumnOids().size(); column_offset++) {
    // Assign all the aggregate variables values corresponding to the current column
    AssignColumnStatistics(context, function, column_offset);

    // @indexIteratorInit(&pg_statistic_index_iterator, queryState.execCtx, num_attrs, table_oid, index_oid,
    // pg_statistic_col_oids)
    InitPgStatisticIterator(function);

    // pg_statistic_index_pr = @indexIteratorGetPR(&pg_statistic_index_iterator)
    // @prSet(pr, type, nullable, attr, expr, false)
    InitPgStatisticIndexPR(function);

    // @indexIteratorScanKey(&pg_statistic_index_iterator)
    auto *scan_call =
        codegen->CallBuiltin(ast::Builtin::IndexIteratorScanKey, {codegen->AddressOf(pg_statistic_index_iterator_)});
    auto *loop_init = codegen->MakeStmt(scan_call);
    // @indexIteratorAdvance(&pg_statistic_index_iterator)
    auto *advance_call =
        codegen->CallBuiltin(ast::Builtin::IndexIteratorAdvance, {codegen->AddressOf(pg_statistic_index_iterator_)});

    // for (@indexIteratorScanKey(&pg_statistic_index_iterator); @indexIteratorAdvance(&pg_statistic_index_iterator); )
    Loop loop(function, loop_init, advance_call, nullptr);
    {
      // var pg_statistic_slot = @indexIteratorGetSlot(&pg_statistic_index_iterator)
      auto pg_statistic_slot = codegen->MakeFreshIdentifier("pg_statistic_slot");
      DeclarePgStatisticSlot(function, pg_statistic_slot);

      // This is where the updating begins

      // var pg_statistic_updater: StorageInterface
      // @storageInterfaceInit(pg_statistic_updater, execCtx, table_oid, pg_statistic_col_oids, true)
      DeclareAndInitPgStatisticUpdater(function);
      // var pg_statistic_update_pr: *ProjectedRow
      DeclarePgStatisticUpdatePr(function);

      // if (!@tableDelete(&pg_statistic_updater, &pg_statistic_slot)) { Abort(); }
      DeleteFromPgStatisticTable(function, pg_statistic_slot);

      // pg_statistic_update_pr = @getTablePR(&pg_statistic_updater)
      InitPgStatisticUpdatePR(function);

      // For each set clause, @prSet(pg_statistic_update_pr, ...)
      SetPgStatisticTablePr(function);

      // var insert_slot = @tableInsert(&pg_statistic_updater)
      InsertIntoPgStatisticTable(function);

      for (const auto &index_oid :
           codegen->GetCatalogAccessor()->GetIndexOids(catalog::postgres::PgStatistic::STATISTIC_TABLE_OID)) {
        // var delete_index_pr = @getIndexPR(&pg_statistic_updater, oid)
        // @prSetCall(...)
        // @indexDelete(&pg_statistic_updater, &pg_statistic_slot)
        DeleteFromPgStatisticIndex(function, pg_statistic_slot, index_oid);

        // var insert_index_pr = @getIndexPR(&pg_statistic_updater, oid)
        // @prSetCall(...)
        // if (!@indexInsertUnique(&pg_statistic_updater)) { Abort(); }
        InsertIntoPgStatisticIndex(function, index_oid);
      }

      // @storageInterfaceFree(&pg_statistic_updater)
      FreePgStatisticUpdater(function);
    }
    loop.EndLoop();
    // @indexIteratorFree(&pg_statistic_index_iterator)
    FreePgStatisticIterator(function);
  }
}

void AnalyzeTranslator::SetPgStatisticColOids(FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();
  auto num_cols = pg_statistic_table_schema_.GetColumns().size();
  // var pg_statistic_col_oids: [num_cols]uint32
  function->Append(
      codegen->DeclareVarNoInit(pg_statistic_col_oids_, codegen->ArrayType(num_cols, ast::BuiltinType::Kind::Uint32)));
  for (size_t i = 0; i < num_cols; i++) {
    auto *lhs = codegen->ArrayAccess(pg_statistic_col_oids_, i);
    auto *rhs = codegen->ConstU32(pg_statistic_table_schema_.GetColumn(i).Oid().UnderlyingValue());
    // pg_statistic_col_oids[i] = ...
    function->Append(codegen->Assign(lhs, rhs));
  }
}

void AnalyzeTranslator::InitPgStatisticVariables(WorkContext *context, FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();
  const auto &plan = GetPlanAs<planner::AnalyzePlanNode>();
  // var table_oid = @intToSql(<table_oid>)
  function->Append(codegen->DeclareVarWithInit(table_oid_, codegen->IntToSql(plan.GetTableOid().UnderlyingValue())));
  // var col_oid: Integer
  function->Append(codegen->DeclareVarNoInit(col_oid_, ast::BuiltinType::Kind::Integer));
  // The first aggregate is COUNT(*)
  // var num_rows = @aggResult(queryState.execCtx, &aggRow.agg_term_attr0)
  function->Append(codegen->DeclareVarWithInit(num_rows_, GetChildOutput(context, 0, 0)));

  // var agg_col: type
  for (size_t i = 0; i < catalog::postgres::PgStatisticImpl::NUM_ANALYZE_AGGREGATES; i++) {
    auto col_info = catalog::postgres::PgStatisticImpl::ANALYZE_AGGREGATES.at(i);
    auto type = sql::GetTypeId(pg_statistic_table_schema_.GetColumn(col_info.column_oid_).Type());
    function->Append(codegen->DeclareVarNoInit(aggregate_variables_[i], codegen->TplType(type)));
  }
}

void AnalyzeTranslator::DeclarePgStatisticIterator(FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();
  auto *iter_type = codegen->BuiltinType(ast::BuiltinType::IndexIterator);
  // var pg_statistic_index_iterator : IndexIterator
  function->Append(codegen->DeclareVarNoInit(pg_statistic_index_iterator_, iter_type));
}

void AnalyzeTranslator::DeclarePgStatisticIndexPR(FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();
  // var pg_statistic_index_pr: *ProjectedRow
  function->Append(codegen->DeclareVarNoInit(
      pg_statistic_index_pr_, codegen->PointerType(codegen->BuiltinType(ast::BuiltinType::Kind::ProjectedRow))));
}

void AnalyzeTranslator::AssignColumnStatistics(WorkContext *context, FunctionBuilder *function,
                                               size_t column_offset) const {
  auto *codegen = GetCodeGen();
  const auto &plan = GetPlanAs<planner::AnalyzePlanNode>();

  // col_oid = @intToSql(<col_oid>)
  auto col_oid = plan.GetColumnOids().at(column_offset).UnderlyingValue();
  function->Append(codegen->Assign(codegen->MakeExpr(col_oid_), codegen->IntToSql(col_oid)));

  for (size_t i = 0; i < catalog::postgres::PgStatisticImpl::NUM_ANALYZE_AGGREGATES; i++) {
    // Offset into the row of aggregates
    size_t agg_offset = (column_offset * catalog::postgres::PgStatisticImpl::NUM_ANALYZE_AGGREGATES) + i + 1;
    auto agg_var = aggregate_variables_.at(i);
    auto *lhs = codegen->MakeExpr(agg_var);
    auto *rhs = GetChildOutput(context, 0, agg_offset);
    // agg_var = @aggResult(queryState.execCtx, &aggRow.agg_term_attr<agg_offset>)
    function->Append(codegen->Assign(lhs, rhs));
  }
}

void AnalyzeTranslator::InitPgStatisticIterator(FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();
  // @indexIteratorInit(&pg_statistic_index_iterator, queryState.execCtx, num_attrs, table_oid, index_oid,
  // pg_statistic_col_oids)
  auto *init_call = codegen->IndexIteratorInit(
      pg_statistic_index_iterator_, GetCompilationContext()->GetExecutionContextPtrFromQueryState(),
      pg_statistic_index_schema_.GetColumns().size(),
      catalog::postgres::PgStatistic::STATISTIC_TABLE_OID.UnderlyingValue(),
      catalog::postgres::PgStatistic::STATISTIC_OID_INDEX_OID.UnderlyingValue(), pg_statistic_col_oids_);
  function->Append(codegen->MakeStmt(init_call));
}

void AnalyzeTranslator::InitPgStatisticIndexPR(FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();
  //  pg_statistic_index_pr = @indexIteratorGetPR(&pg_statistic_index_iterator)
  auto *get_pr_call =
      codegen->CallBuiltin(ast::Builtin::IndexIteratorGetPR, {codegen->AddressOf(pg_statistic_index_iterator_)});
  function->Append(codegen->Assign(codegen->MakeExpr(pg_statistic_index_pr_), get_pr_call));
  // @prSet(pr, type, nullable, attr, expr, false)
  FillIndexPrKey(function, pg_statistic_index_pr_, false);
}

void AnalyzeTranslator::DeclarePgStatisticSlot(FunctionBuilder *function, ast::Identifier slot) const {
  auto *codegen = GetCodeGen();
  // var pg_statistic_slot = @indexIteratorGetSlot(&pg_statistic_index_iterator)
  auto *get_slot_call =
      codegen->CallBuiltin(ast::Builtin::IndexIteratorGetSlot, {codegen->AddressOf(pg_statistic_index_iterator_)});
  function->Append(codegen->DeclareVarWithInit(slot, get_slot_call));
}

void AnalyzeTranslator::DeclareAndInitPgStatisticUpdater(FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();
  // var pg_statistic_updater: StorageInterface
  auto *storage_interface_type = codegen->BuiltinType(ast::BuiltinType::Kind::StorageInterface);
  function->Append(codegen->DeclareVarNoInit(pg_statistic_updater_, storage_interface_type));
  // @storageInterfaceInit(pg_statistic_updater, execCtx, table_oid, pg_statistic_col_oids, true)
  auto *updater_setup = codegen->StorageInterfaceInit(
      pg_statistic_updater_, GetExecutionContext(),
      catalog::postgres::PgStatistic::STATISTIC_TABLE_OID.UnderlyingValue(), pg_statistic_col_oids_, true);
  function->Append(codegen->MakeStmt(updater_setup));
}

void AnalyzeTranslator::DeclarePgStatisticUpdatePr(FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();
  // var pg_statistic_update_pr: *ProjectedRow
  auto *pr_type = codegen->BuiltinType(ast::BuiltinType::Kind::ProjectedRow);
  function->Append(codegen->DeclareVarNoInit(pg_statistic_update_pr_, codegen->PointerType(pr_type)));
}

void AnalyzeTranslator::DeleteFromPgStatisticTable(FunctionBuilder *function, ast::Identifier slot) const {
  auto *codegen = GetCodeGen();
  // if (!@tableDelete(&pg_statistic_updater, &pg_statistic_slot)) { Abort(); }
  auto *delete_slot = codegen->AddressOf(codegen->MakeExpr(slot));
  auto *delete_call =
      codegen->CallBuiltin(ast::Builtin::TableDelete, {codegen->AddressOf(pg_statistic_updater_), delete_slot});
  auto *delete_failed = codegen->UnaryOp(parsing::Token::Type::BANG, delete_call);
  If check(function, delete_failed);
  {
    // The delete was not successful; abort the transaction.
    function->Append(codegen->AbortTxn(GetExecutionContext()));
  }
  check.EndIf();
}

void AnalyzeTranslator::InitPgStatisticUpdatePR(FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();
  // pg_statistic_update_pr = @getTablePR(&pg_statistic_updater)
  auto *get_table_pr_call = codegen->CallBuiltin(ast::Builtin::GetTablePR, {codegen->AddressOf(pg_statistic_updater_)});
  function->Append(codegen->Assign(codegen->MakeExpr(pg_statistic_update_pr_), get_table_pr_call));
}

void AnalyzeTranslator::SetPgStatisticTablePr(FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();

  // For each set clause, @prSet(pg_statistic_update_pr, ...)
  for (size_t i = 0; i < pg_statistic_table_schema_.GetColumns().size(); i++) {
    const auto &col = pg_statistic_table_schema_.GetColumn(i);
    auto var = pg_statistic_column_lookup_.find(col.Oid());
    auto *var_expr = codegen->MakeExpr(var->second);
    auto *pr_set_call = codegen->PRSet(codegen->MakeExpr(pg_statistic_update_pr_), col.Type(), col.Nullable(),
                                       pg_statistic_table_pm_.find(col.Oid())->second, var_expr, true);
    function->Append(codegen->MakeStmt(pr_set_call));
  }
}

void AnalyzeTranslator::InsertIntoPgStatisticTable(FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();
  // var insert_slot = @tableInsert(&updater_)
  auto insert_slot = codegen->MakeFreshIdentifier("insert_slot");
  auto *insert_call = codegen->CallBuiltin(ast::Builtin::TableInsert, {codegen->AddressOf(pg_statistic_updater_)});
  function->Append(codegen->DeclareVar(insert_slot, nullptr, insert_call));
}

void AnalyzeTranslator::DeleteFromPgStatisticIndex(FunctionBuilder *function, ast::Identifier slot,
                                                   catalog::index_oid_t index_oid) const {
  auto *codegen = GetCodeGen();
  // var delete_index_pr = @getIndexPR(&pg_statistic_updater, oid)
  auto delete_index_pr = codegen->MakeFreshIdentifier("delete_index_pr");
  std::vector<ast::Expr *> pr_delete_call_args{codegen->AddressOf(pg_statistic_updater_),
                                               codegen->Const32(index_oid.UnderlyingValue())};
  auto *get_index_pr_delete_call = codegen->CallBuiltin(ast::Builtin::GetIndexPR, pr_delete_call_args);
  function->Append(codegen->DeclareVarWithInit(delete_index_pr, get_index_pr_delete_call));

  // @prSetCall(delete_index_pr, type, nullable, attr_idx, val)
  FillIndexPrKey(function, delete_index_pr, true);

  // @indexDelete(&pg_statistic_updater, &pg_statistic_slot)
  std::vector<ast::Expr *> delete_args{codegen->AddressOf(pg_statistic_updater_),
                                       codegen->AddressOf(codegen->MakeExpr(slot))};
  auto *index_delete_call = codegen->CallBuiltin(ast::Builtin::IndexDelete, delete_args);
  function->Append(codegen->MakeStmt(index_delete_call));
}

void AnalyzeTranslator::InsertIntoPgStatisticIndex(FunctionBuilder *function, catalog::index_oid_t index_oid) const {
  auto *codegen = GetCodeGen();
  // var insert_index_pr = @getIndexPR(&pg_statistic_updater, oid)
  const auto &insert_index_pr = codegen->MakeFreshIdentifier("insert_index_pr");
  std::vector<ast::Expr *> pr_insert_call_args{codegen->AddressOf(pg_statistic_updater_),
                                               codegen->Const32(index_oid.UnderlyingValue())};
  auto *get_index_pr_insert_call = codegen->CallBuiltin(ast::Builtin::GetIndexPR, pr_insert_call_args);
  function->Append(codegen->DeclareVarWithInit(insert_index_pr, get_index_pr_insert_call));

  // @prSet(insert_index_pr, attr_idx, val, true)
  FillIndexPrKey(function, insert_index_pr, true);

  // if (!@indexInsertUnique(&pg_statistic_updater)) { Abort(); }
  const auto &builtin =
      pg_statistic_index_schema_.Unique() ? ast::Builtin::IndexInsertUnique : ast::Builtin::IndexInsert;
  auto *index_insert_call = codegen->CallBuiltin(builtin, {codegen->AddressOf(pg_statistic_updater_)});
  auto *cond = codegen->UnaryOp(parsing::Token::Type::BANG, index_insert_call);
  If success(function, cond);
  { function->Append(codegen->AbortTxn(GetExecutionContext())); }
  success.EndIf();
}

void AnalyzeTranslator::FreePgStatisticUpdater(FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();
  // @storageInterfaceFree(&pg_statistic_updater)
  auto *updater_free =
      codegen->CallBuiltin(ast::Builtin::StorageInterfaceFree, {codegen->AddressOf(pg_statistic_updater_)});
  function->Append(codegen->MakeStmt(updater_free));
}

void AnalyzeTranslator::FreePgStatisticIterator(FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();
  // @indexIteratorFree(&pg_statistic_index_iterator)
  auto *free_call =
      codegen->CallBuiltin(ast::Builtin::IndexIteratorFree, {codegen->AddressOf(pg_statistic_index_iterator_)});
  function->Append(codegen->MakeStmt(free_call));
}

void AnalyzeTranslator::FillIndexPrKey(FunctionBuilder *function, ast::Identifier index_projected_row, bool own) const {
  auto *codegen = GetCodeGen();

  for (const auto &col : pg_statistic_index_schema_.GetColumns()) {
    auto attr_type = col.Type();
    auto nullable = col.Nullable();
    auto col_expr = col.StoredExpression();
    NOISEPAGE_ASSERT(col_expr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE,
                     "Index column stored expression should be a column value expression");
    auto col_cve = col_expr.CastManagedPointerTo<const parser::ColumnValueExpression>();
    auto var = pg_statistic_column_lookup_.find(col_cve->GetColumnOid());
    auto *var_expr = codegen->MakeExpr(var->second);
    auto *set_key_call = codegen->PRSet(codegen->MakeExpr(index_projected_row), attr_type, nullable,
                                        pg_statistic_index_pm_.at(col.Oid()), var_expr, own);
    // @prSet(pr, type, nullable, attr, expr, true)
    function->Append(codegen->MakeStmt(set_key_call));
  }
}

util::RegionVector<ast::FieldDecl *> AnalyzeTranslator::GetWorkerParams() const { UNREACHABLE("Analyze is serial."); }

void AnalyzeTranslator::LaunchWork(FunctionBuilder *function, ast::Identifier work_func_name) const {
  UNREACHABLE("Analyze is serial.");
}

ast::Expr *AnalyzeTranslator::GetChildOutput(WorkContext *context, uint32_t child_idx, uint32_t attr_idx) const {
  NOISEPAGE_ASSERT(child_idx == 0, "Analyze plan can only have one child");

  return OperatorTranslator::GetChildOutput(context, child_idx, attr_idx);
}

ast::Expr *AnalyzeTranslator::GetTableColumn(catalog::col_oid_t col_oid) const {
  UNREACHABLE("Analyze doesn't provide values");
}

}  // namespace noisepage::execution::compiler
