#include "execution/compiler/operator/update_translator.h"

#include <execution/compiler/if.h>

#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/work_context.h"
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
      all_oids_(CollectOids(plan)),
      table_pm_(GetCodeGen()->GetCatalogAccessor()->GetTable(plan.GetTableOid())->ProjectionMapForOids(all_oids_))
//      ,
//      pr_filler_(GetCodeGen(), table_schema_, table_pm_, update_pr_)
{
  compilation_context->Prepare(*plan.GetChild(0), pipeline);

  for (auto &index_oid : GetCodeGen()->GetCatalogAccessor()->GetIndexOids(plan.GetTableOid())) {
    auto &index_schema = GetCodeGen()->GetCatalogAccessor()->GetIndexSchema(index_oid);
    for (const auto &index_col : index_schema.GetColumns()) {
      compilation_context->Prepare(*index_col.StoredExpression().Get());
    }
  }

  for (auto &clause : plan.GetSetClauses()) {
    compilation_context->Prepare(*clause.second.Get());
  }
}


//void UpdateTranslator::Abort(FunctionBuilder *builder) {
//  GenUpdaterFree(builder);
//  child_translator_->Abort(builder);
//  builder->Append(GetCodeGen()->ReturnStmt(nullptr));
//}

void UpdateTranslator::PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const {
  DeclareUpdater(function);
  DeclareUpdatePR(function);
  auto &op = GetPlanAs<planner::UpdatePlanNode>();
  if (op.GetIndexedUpdate()) {
    // For indexed updates, we need to call delete first
    GenTableDelete(function);
  }
  GetUpdatePR(function);
  FillPRFromChild(context, function);

  if (op.GetIndexedUpdate()) {
    // Indexed updates re-insert into the table
    GenTableInsert(function);
    // Then they delete and insert into every index.
    // Update into every index
    const auto &indexes = GetCodeGen()->GetCatalogAccessor()->GetIndexOids(op.GetTableOid());
    for (auto &index_oid : indexes) {
      GenIndexDelete(context, function, index_oid);
      GenIndexInsert(context, function, index_oid);
    }
    return;
  }
  // Non indexed updates just update.
  GenTableUpdate(function);
  GenUpdaterFree(function);
}

void UpdateTranslator::DeclareUpdater(terrier::execution::compiler::FunctionBuilder *builder) const {
  // Generate col oids
  auto &op = GetPlanAs<planner::UpdatePlanNode>();
  SetOids(builder);
  // var updater : StorageInterface
  auto storage_interface_type = GetCodeGen()->BuiltinType(ast::BuiltinType::Kind::StorageInterface);
  builder->Append(GetCodeGen()->DeclareVar(updater_, storage_interface_type, nullptr));
  // Call @storageInterfaceInit
  ast::Expr *updater_setup = GetCodeGen()->StorageInterfaceInit(updater_, GetExecutionContext(),
                                                                !op.GetTableOid(), col_oids_, true);
  builder->Append(GetCodeGen()->MakeStmt(updater_setup));
}

void UpdateTranslator::GenUpdaterFree(terrier::execution::compiler::FunctionBuilder *builder) const {
  // Call @storageInterfaceFree
  ast::Expr *updater_free = GetCodeGen()->CallBuiltin(ast::Builtin::StorageInterfaceFree,
                                                      {GetCodeGen()->AddressOf(updater_)});
  builder->Append(GetCodeGen()->MakeStmt(updater_free));
}

ast::Expr *UpdateTranslator::GetChildOutput(WorkContext *context, uint32_t child_idx, uint32_t attr_idx) const {
  TERRIER_ASSERT(child_idx == 0, "Update plan can only have one child");
  auto &op = GetPlanAs<planner::UpdatePlanNode>();
  auto child_translator = GetCompilationContext()->LookupTranslator(*op.GetChild(0));
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
  // Declare: var col_oids: [num_cols]uint32
  ast::Expr *arr_type = GetCodeGen()->ArrayType(all_oids_.size(), ast::BuiltinType::Kind::Uint32);
  builder->Append(GetCodeGen()->DeclareVar(col_oids_, arr_type, nullptr));

  // For each oid, set col_oids[i] = col_oid
  for (uint16_t i = 0; i < all_oids_.size(); i++) {
    ast::Expr *lhs = GetCodeGen()->ArrayAccess(col_oids_, i);
    ast::Expr *rhs = GetCodeGen()->Const32(!all_oids_[i]);
    builder->Append(GetCodeGen()->Assign(lhs, rhs));
  }
}

void UpdateTranslator::DeclareUpdatePR(terrier::execution::compiler::FunctionBuilder *builder) const {
  // var update_pr : *ProjectedRow
  auto pr_type = GetCodeGen()->BuiltinType(ast::BuiltinType::Kind::ProjectedRow);
  builder->Append(GetCodeGen()->DeclareVar(update_pr_, GetCodeGen()->PointerType(pr_type), nullptr));
}

void UpdateTranslator::GetUpdatePR(terrier::execution::compiler::FunctionBuilder *builder) const {
  // var update_pr = ProjectedRow
  auto get_pr_call = GetCodeGen()->CallBuiltin(ast::Builtin::GetTablePR, {GetCodeGen()->AddressOf(updater_)});
  builder->Append(GetCodeGen()->Assign(GetCodeGen()->MakeExpr(update_pr_), get_pr_call));
}

void UpdateTranslator::FillPRFromChild(WorkContext *context,
                                       terrier::execution::compiler::FunctionBuilder *builder) const {
  auto &op = GetPlanAs<planner::UpdatePlanNode>();
  const auto &clauses = op.GetSetClauses();

  for (const auto &clause : clauses) {
    const auto &table_col_oid = clause.first;
    const auto &table_col = table_schema_.GetColumn(table_col_oid);
    auto clause_expr = context->DeriveValue(*clause.second.Get(), this);
    auto pr_set_call = GetCodeGen()->PRSet(GetCodeGen()->MakeExpr(update_pr_), table_col.Type(), table_col.Nullable(),
                                       table_pm_.find(table_col_oid)->second, clause_expr);
    builder->Append(GetCodeGen()->MakeStmt(pr_set_call));
  }
}

void UpdateTranslator::GenTableUpdate(FunctionBuilder *builder) const {
  //   if (update fails) { Abort(); }
  auto &op = GetPlanAs<planner::UpdatePlanNode>();
  auto child_translator = GetCompilationContext()->LookupTranslator(*op.GetChild(0));
  auto update_slot = child_translator->GetSlot();
  std::vector<ast::Expr *> update_args{GetCodeGen()->AddressOf(updater_), update_slot};
  auto update_call = GetCodeGen()->CallBuiltin(ast::Builtin::TableUpdate, std::move(update_args));

  auto cond = GetCodeGen()->UnaryOp(parsing::Token::Type::BANG, update_call);
  If success(builder, cond);
  builder->Append(GetCodeGen()->AbortTxn(GetExecutionContext()));
  success.EndIf();
}

void UpdateTranslator::GenTableInsert(FunctionBuilder *builder) const {
  // var insert_slot = @tableInsert(&updater_)
  auto insert_slot = GetCodeGen()->MakeFreshIdentifier("insert_slot");
  auto insert_call = GetCodeGen()->CallBuiltin(ast::Builtin::TableInsert,
                                               {GetCodeGen()->AddressOf(updater_)});

  builder->Append(GetCodeGen()->DeclareVar(insert_slot, nullptr, insert_call));
}

void UpdateTranslator::GenIndexInsert(WorkContext *context,
                                      FunctionBuilder *builder, const catalog::index_oid_t &index_oid) const {
  // var insert_index_pr = @getIndexPR(&inserter, oid)
  auto insert_index_pr = GetCodeGen()->MakeFreshIdentifier("insert_index_pr");
  std::vector<ast::Expr *> pr_call_args{GetCodeGen()->AddressOf(updater_), GetCodeGen()->Const32(!index_oid)};
  auto get_index_pr_call = GetCodeGen()->CallBuiltin(ast::Builtin::GetIndexPR, std::move(pr_call_args));
  builder->Append(GetCodeGen()->DeclareVar(insert_index_pr, nullptr, get_index_pr_call));

  // Fill up the index pr from the update_pr
  auto index = GetCodeGen()->GetCatalogAccessor()->GetIndex(index_oid);
  const auto &index_pm = index->GetKeyOidToOffsetMap();
  const auto &index_schema = GetCodeGen()->GetCatalogAccessor()->GetIndexSchema(index_oid);

//  pr_filler_.GenFiller(index_pm, index_schema, GetCodeGen()->MakeExpr(insert_index_pr), builder);

  // Fill index_pr using table_pr
  for (const auto &index_col : index_schema.GetColumns()) {
    auto col_expr = context->DeriveValue(*index_col.StoredExpression().Get(), this);
    uint16_t attr_offset = index_pm.at(index_col.Oid());
    type::TypeId attr_type = index_col.Type();
    bool nullable = index_col.Nullable();
    auto set_key_call =
        GetCodeGen()->PRSet(GetCodeGen()->MakeExpr(insert_index_pr), attr_type, nullable, attr_offset, col_expr);
    builder->Append(GetCodeGen()->MakeStmt(set_key_call));
  }

  // Insert into index
  // if (insert not successfull) { Abort(); }
  auto index_insert_call = GetCodeGen()->CallBuiltin(
      index_schema.Unique() ? ast::Builtin::IndexInsertUnique : ast::Builtin::IndexInsert,
      {GetCodeGen()->AddressOf(updater_)});
  auto cond = GetCodeGen()->UnaryOp(parsing::Token::Type::BANG, index_insert_call);
  If success(builder, cond);
//  Abort(builder);
  success.EndIf();
}

void UpdateTranslator::GenTableDelete(FunctionBuilder *builder) const {
  // Delete from table
  // if (delete not successfull) { Abort(); }
  auto &op = GetPlanAs<planner::UpdatePlanNode>();
  auto child_translator = GetCompilationContext()->LookupTranslator(*op.GetChild(0));
  auto delete_slot = child_translator->GetSlot();
  std::vector<ast::Expr *> delete_args{GetCodeGen()->AddressOf(updater_), delete_slot};
  auto delete_call = GetCodeGen()->CallBuiltin(ast::Builtin::TableDelete, std::move(delete_args));
  auto cond = GetCodeGen()->UnaryOp(parsing::Token::Type::BANG, delete_call);
  If success(builder, cond);
//  Abort(builder);
  success.EndIf();
}

void UpdateTranslator::GenIndexDelete(WorkContext *context,
                                      FunctionBuilder *builder, const catalog::index_oid_t &index_oid) const {
  // var delete_index_pr = @getIndexPR(&deleter, oid)
  auto delete_index_pr = GetCodeGen()->MakeFreshIdentifier("delete_index_pr");
  std::vector<ast::Expr *> pr_call_args{GetCodeGen()->AddressOf(updater_), GetCodeGen()->Const32(!index_oid)};
  auto get_index_pr_call = GetCodeGen()->CallBuiltin(ast::Builtin::GetIndexPR, std::move(pr_call_args));

  builder->Append(GetCodeGen()->DeclareVar(delete_index_pr, nullptr, get_index_pr_call));

  // Fill up the index pr
  auto index = GetCodeGen()->GetCatalogAccessor()->GetIndex(index_oid);
  const auto &index_pm = index->GetKeyOidToOffsetMap();
  const auto &index_schema = GetCodeGen()->GetCatalogAccessor()->GetIndexSchema(index_oid);
  const auto &index_cols = index_schema.GetColumns();
  auto &op = GetPlanAs<planner::UpdatePlanNode>();
  auto child_translator = GetCompilationContext()->LookupTranslator(*op.GetChild(0));
  for (const auto &index_col : index_cols) {
    // NOTE: index expressions in delete refer to columns in the child translator (before any update is done).
    // For example, if the child is a seq scan, the index expressions could contain ColumnValueExpressions.

    auto val = context->DeriveValue(*index_col.StoredExpression().Get(), child_translator);
    auto pr_set_call = GetCodeGen()->PRSet(GetCodeGen()->MakeExpr(delete_index_pr), index_col.Type(), index_col.Nullable(),
                                       index_pm.at(index_col.Oid()), val);
    builder->Append(GetCodeGen()->MakeStmt(pr_set_call));
  }

  // Delete from index
  std::vector<ast::Expr *> delete_args{GetCodeGen()->AddressOf(updater_), child_translator->GetSlot()};
  auto index_delete_call = GetCodeGen()->CallBuiltin(ast::Builtin::IndexDelete, std::move(delete_args));
  builder->Append(GetCodeGen()->MakeStmt(index_delete_call));
}

}  // namespace terrier::execution::compiler
