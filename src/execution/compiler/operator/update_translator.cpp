#include "execution/compiler/operator/update_translator.h"

#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/if.h"
#include "execution/compiler/work_context.h"
#include "planner/plannodes/update_plan_node.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"

namespace terrier::execution::compiler {
UpdateTranslator::UpdateTranslator(const planner::UpdatePlanNode &plan, CompilationContext *compilation_context,
                                   Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline, brain::ExecutionOperatingUnitType::UPDATE),
      updater_(GetCodeGen()->MakeFreshIdentifier("updater")),
      update_pr_(GetCodeGen()->MakeFreshIdentifier("update_pr")),
      col_oids_(GetCodeGen()->MakeFreshIdentifier("col_oids")),
      table_schema_(GetCodeGen()->GetCatalogAccessor()->GetSchema(plan.GetTableOid())),
      all_oids_(CollectOids(table_schema_)),
      table_pm_(GetCodeGen()->GetCatalogAccessor()->GetTable(plan.GetTableOid())->ProjectionMapForOids(all_oids_)) {
  pipeline->RegisterSource(this, Pipeline::Parallelism::Serial);
  compilation_context->Prepare(*plan.GetChild(0), pipeline);

  for (const auto &clause : plan.GetSetClauses()) {
    compilation_context->Prepare(*clause.second);
  }

  for (auto &index_oid : GetCodeGen()->GetCatalogAccessor()->GetIndexOids(plan.GetTableOid())) {
    const auto &index_schema = GetCodeGen()->GetCatalogAccessor()->GetIndexSchema(index_oid);
    for (const auto &index_col : index_schema.GetColumns()) {
      compilation_context->Prepare(*index_col.StoredExpression());
    }
  }
}

void UpdateTranslator::PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const {
  // var col_oids: [num_cols]uint32
  // col_oids[i] = ...
  // var updater : StorageInterface
  // @storageInterfaceInit(updater, execCtx, table_oid, col_oids, true)
  DeclareUpdater(function);
  // var update_pr : *ProjectedRow
  DeclareUpdatePR(function);

  const auto &op = GetPlanAs<planner::UpdatePlanNode>();

  if (op.GetIndexedUpdate()) {
    // For indexed updates, we need to call delete first.
    // if (!@tableDelete(&deleter, &slot)) { Abort(); }
    GenTableDelete(function);
  }

  // var update_pr = @getTablePR(&updater)
  // @prSet(update_pr, ... @vpiGet(...) ...)
  GetUpdatePR(function);

  // For each set clause, @prSet(update_pr, ...)
  GenSetTablePR(function, context);

  if (op.GetIndexedUpdate()) {
    // For indexed updates, we need to re-insert into the table, and then delete-and-insert into every index.
    // var insert_slot = @tableInsert(&updater_)
    GenTableInsert(function);
    const auto &indexes = GetCodeGen()->GetCatalogAccessor()->GetIndexOids(op.GetTableOid());
    for (const auto &index_oid : indexes) {
      GenIndexDelete(function, context, index_oid);
      GenIndexInsert(context, function, index_oid);
    }
  } else {
    // Non-indexed updates just update.
    GenTableUpdate(function);
  }
  function->Append(GetCodeGen()->ExecCtxAddRowsAffected(GetExecutionContext(), 1));

  // @storageInterfaceFree(&updater)
  GenUpdaterFree(function);
}

void UpdateTranslator::DeclareUpdater(terrier::execution::compiler::FunctionBuilder *builder) const {
  // var col_oids: [num_cols]uint32
  // col_oids[i] = ...
  SetOids(builder);
  // var updater : StorageInterface
  auto *storage_interface_type = GetCodeGen()->BuiltinType(ast::BuiltinType::Kind::StorageInterface);
  builder->Append(GetCodeGen()->DeclareVar(updater_, storage_interface_type, nullptr));
  // @storageInterfaceInit(updater, execCtx, table_oid, col_oids, true)
  ast::Expr *updater_setup = GetCodeGen()->StorageInterfaceInit(
      updater_, GetExecutionContext(), GetPlanAs<planner::UpdatePlanNode>().GetTableOid().UnderlyingValue(), col_oids_,
      true);
  builder->Append(GetCodeGen()->MakeStmt(updater_setup));
}

void UpdateTranslator::GenUpdaterFree(terrier::execution::compiler::FunctionBuilder *builder) const {
  // @storageInterfaceFree(&updater)
  ast::Expr *updater_free =
      GetCodeGen()->CallBuiltin(ast::Builtin::StorageInterfaceFree, {GetCodeGen()->AddressOf(updater_)});
  builder->Append(GetCodeGen()->MakeStmt(updater_free));
}

ast::Expr *UpdateTranslator::GetChildOutput(WorkContext *context, uint32_t child_idx, uint32_t attr_idx) const {
  TERRIER_ASSERT(child_idx == 0, "Update plan can only have one child");
  const auto &op = GetPlanAs<planner::UpdatePlanNode>();
  const auto &child_translator = GetCompilationContext()->LookupTranslator(*op.GetChild(0));
  return child_translator->GetOutput(context, attr_idx);
}

ast::Expr *UpdateTranslator::GetTableColumn(catalog::col_oid_t col_oid) const {
  auto column = table_schema_.GetColumn(col_oid);
  auto type = column.Type();
  auto nullable = column.Nullable();
  auto attr_index = table_pm_.find(col_oid)->second;
  return GetCodeGen()->PRGet(GetCodeGen()->MakeExpr(update_pr_), type, nullable, attr_index);
}

void UpdateTranslator::SetOids(FunctionBuilder *builder) const {
  // var col_oids: [num_cols]uint32
  ast::Expr *arr_type = GetCodeGen()->ArrayType(all_oids_.size(), ast::BuiltinType::Kind::Uint32);
  builder->Append(GetCodeGen()->DeclareVar(col_oids_, arr_type, nullptr));

  for (uint16_t i = 0; i < all_oids_.size(); i++) {
    // col_oids[i] = col_oid
    ast::Expr *lhs = GetCodeGen()->ArrayAccess(col_oids_, i);
    ast::Expr *rhs = GetCodeGen()->Const32(all_oids_[i].UnderlyingValue());
    builder->Append(GetCodeGen()->Assign(lhs, rhs));
  }
}

void UpdateTranslator::DeclareUpdatePR(terrier::execution::compiler::FunctionBuilder *builder) const {
  // var update_pr : *ProjectedRow
  auto *pr_type = GetCodeGen()->BuiltinType(ast::BuiltinType::Kind::ProjectedRow);
  builder->Append(GetCodeGen()->DeclareVar(update_pr_, GetCodeGen()->PointerType(pr_type), nullptr));
}

void UpdateTranslator::GetUpdatePR(terrier::execution::compiler::FunctionBuilder *builder) const {
  // var update_pr = @getTablePR(&updater)
  auto *get_pr_call = GetCodeGen()->CallBuiltin(ast::Builtin::GetTablePR, {GetCodeGen()->AddressOf(updater_)});
  builder->Append(GetCodeGen()->Assign(GetCodeGen()->MakeExpr(update_pr_), get_pr_call));

  const auto &op = GetPlanAs<planner::UpdatePlanNode>();
  auto *update_pr = GetCodeGen()->MakeExpr(update_pr_);
  // TODO(WAN): is this a hack?
  // Set the update_pr from the child so that updates can safely refer to themselves, e.g. UPDATE a = a+1.
  // @prSet(update_pr, ...)
  if (op.GetChildrenSize() > 0) {
    const auto *child = GetCompilationContext()->LookupTranslator(*op.GetChild(0));

    for (const auto oid : all_oids_) {
      const auto &col = table_schema_.GetColumn(oid);
      const auto idx = table_pm_.find(oid)->second;

      ast::Expr *child_expr = child->GetTableColumn(oid);
      ast::Expr *set_pr = GetCodeGen()->PRSet(update_pr, col.Type(), col.Nullable(), idx, child_expr, true);
      builder->Append(GetCodeGen()->MakeStmt(set_pr));
    }
  }
}

void UpdateTranslator::GenSetTablePR(FunctionBuilder *builder, WorkContext *context) const {
  const auto &clauses = GetPlanAs<planner::UpdatePlanNode>().GetSetClauses();

  for (const auto &clause : clauses) {
    // @prSet(update_pr, ...)
    const auto &table_col_oid = clause.first;
    const auto &table_col = table_schema_.GetColumn(table_col_oid);
    const auto &clause_expr = context->DeriveValue(*clause.second, this);
    auto *pr_set_call = GetCodeGen()->PRSet(GetCodeGen()->MakeExpr(update_pr_), table_col.Type(), table_col.Nullable(),
                                            table_pm_.find(table_col_oid)->second, clause_expr, true);
    builder->Append(GetCodeGen()->MakeStmt(pr_set_call));
  }
}

void UpdateTranslator::GenTableUpdate(FunctionBuilder *builder) const {
  // if (!tableUpdate(&updater) { Abort(); }
  const auto &op = GetPlanAs<planner::UpdatePlanNode>();
  const auto &child_translator = GetCompilationContext()->LookupTranslator(*op.GetChild(0));
  const auto &update_slot = child_translator->GetSlotAddress();
  std::vector<ast::Expr *> update_args{GetCodeGen()->AddressOf(updater_), update_slot};
  auto *update_call = GetCodeGen()->CallBuiltin(ast::Builtin::TableUpdate, update_args);

  auto *cond = GetCodeGen()->UnaryOp(parsing::Token::Type::BANG, update_call);
  If success(builder, cond);
  builder->Append(GetCodeGen()->AbortTxn(GetExecutionContext()));
  success.EndIf();
}

void UpdateTranslator::GenTableInsert(FunctionBuilder *builder) const {
  // var insert_slot = @tableInsert(&updater_)
  const auto &insert_slot = GetCodeGen()->MakeFreshIdentifier("insert_slot");
  auto *insert_call = GetCodeGen()->CallBuiltin(ast::Builtin::TableInsert, {GetCodeGen()->AddressOf(updater_)});
  builder->Append(GetCodeGen()->DeclareVar(insert_slot, nullptr, insert_call));
}

void UpdateTranslator::GenIndexInsert(WorkContext *context, FunctionBuilder *builder,
                                      const catalog::index_oid_t &index_oid) const {
  // var insert_index_pr = @getIndexPR(&updater, oid)
  const auto &insert_index_pr = GetCodeGen()->MakeFreshIdentifier("insert_index_pr");
  std::vector<ast::Expr *> pr_call_args{GetCodeGen()->AddressOf(updater_),
                                        GetCodeGen()->Const32(index_oid.UnderlyingValue())};
  auto *get_index_pr_call = GetCodeGen()->CallBuiltin(ast::Builtin::GetIndexPR, pr_call_args);
  builder->Append(GetCodeGen()->DeclareVar(insert_index_pr, nullptr, get_index_pr_call));

  const auto &index = GetCodeGen()->GetCatalogAccessor()->GetIndex(index_oid);
  const auto &index_pm = index->GetKeyOidToOffsetMap();
  const auto &index_schema = GetCodeGen()->GetCatalogAccessor()->GetIndexSchema(index_oid);
  auto *index_pr_expr = GetCodeGen()->MakeExpr(insert_index_pr);

  for (const auto &index_col : index_schema.GetColumns()) {
    // @prSet(insert_index_pr, attr_idx, val, true)
    const auto &col_expr = context->DeriveValue(*index_col.StoredExpression().Get(), this);
    uint16_t attr_offset = index_pm.at(index_col.Oid());
    type::TypeId attr_type = index_col.Type();
    bool nullable = index_col.Nullable();
    auto *set_key_call = GetCodeGen()->PRSet(index_pr_expr, attr_type, nullable, attr_offset, col_expr, true);
    builder->Append(GetCodeGen()->MakeStmt(set_key_call));
  }

  // if (!@indexInsert(&updater)) { Abort(); }
  const auto &builtin = index_schema.Unique() ? ast::Builtin::IndexInsertUnique : ast::Builtin::IndexInsert;
  auto *index_insert_call = GetCodeGen()->CallBuiltin(builtin, {GetCodeGen()->AddressOf(updater_)});
  auto *cond = GetCodeGen()->UnaryOp(parsing::Token::Type::BANG, index_insert_call);
  If success(builder, cond);
  { builder->Append(GetCodeGen()->AbortTxn(GetExecutionContext())); }
  success.EndIf();
}

void UpdateTranslator::GenTableDelete(FunctionBuilder *builder) const {
  // if (!@tableDelete(&deleter, &slot)) { Abort(); }
  const auto &op = GetPlanAs<planner::UpdatePlanNode>();
  const auto &child = GetCompilationContext()->LookupTranslator(*op.GetChild(0));
  TERRIER_ASSERT(child != nullptr, "delete should have a child");
  const auto &delete_slot = child->GetSlotAddress();
  std::vector<ast::Expr *> delete_args{GetCodeGen()->AddressOf(updater_), delete_slot};
  auto *delete_call = GetCodeGen()->CallBuiltin(ast::Builtin::TableDelete, delete_args);
  auto *delete_failed = GetCodeGen()->UnaryOp(parsing::Token::Type::BANG, delete_call);
  If check(builder, delete_failed);
  {
    // The delete was not successful; abort the transaction.
    builder->Append(GetCodeGen()->AbortTxn(GetExecutionContext()));
  }
  check.EndIf();
}

void UpdateTranslator::GenIndexDelete(FunctionBuilder *builder, WorkContext *context,
                                      const catalog::index_oid_t &index_oid) const {
  // var delete_index_pr = @getIndexPR(&updater, oid)
  auto delete_index_pr = GetCodeGen()->MakeFreshIdentifier("delete_index_pr");
  std::vector<ast::Expr *> pr_call_args{GetCodeGen()->AddressOf(updater_),
                                        GetCodeGen()->Const32(index_oid.UnderlyingValue())};
  auto *get_index_pr_call = GetCodeGen()->CallBuiltin(ast::Builtin::GetIndexPR, pr_call_args);
  builder->Append(GetCodeGen()->DeclareVar(delete_index_pr, nullptr, get_index_pr_call));

  auto index = GetCodeGen()->GetCatalogAccessor()->GetIndex(index_oid);
  const auto &index_pm = index->GetKeyOidToOffsetMap();
  const auto &index_schema = GetCodeGen()->GetCatalogAccessor()->GetIndexSchema(index_oid);
  const auto &index_cols = index_schema.GetColumns();

  const auto &op = GetPlanAs<planner::UpdatePlanNode>();
  const auto &child = GetCompilationContext()->LookupTranslator(*op.GetChild(0));
  for (const auto &index_col : index_cols) {
    // @prSetCall(delete_index_pr, type, nullable, attr_idx, val)
    // NOTE: index expressions refer to columns in the child translator.
    // For example, if the child is a seq scan, the index expressions would contain ColumnValueExpressions
    const auto &val = context->DeriveValue(*index_col.StoredExpression().Get(), child);
    auto *pr_set_call = GetCodeGen()->PRSet(GetCodeGen()->MakeExpr(delete_index_pr), index_col.Type(),
                                            index_col.Nullable(), index_pm.at(index_col.Oid()), val, true);
    builder->Append(GetCodeGen()->MakeStmt(pr_set_call));
  }

  // @indexDelete(&updater)
  std::vector<ast::Expr *> delete_args{GetCodeGen()->AddressOf(updater_), child->GetSlotAddress()};
  auto *index_delete_call = GetCodeGen()->CallBuiltin(ast::Builtin::IndexDelete, delete_args);
  builder->Append(GetCodeGen()->MakeStmt(index_delete_call));
}

std::vector<catalog::col_oid_t> UpdateTranslator::CollectOids(const catalog::Schema &schema) {
  std::vector<catalog::col_oid_t> oids;
  for (const auto &col : schema.GetColumns()) {
    oids.emplace_back(col.Oid());
  }
  return oids;
}

}  // namespace terrier::execution::compiler
