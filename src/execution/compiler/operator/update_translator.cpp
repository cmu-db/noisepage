#include "execution/compiler/operator/update_translator.h"

#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/translator_factory.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"

namespace terrier::execution::compiler {
UpdateTranslator::UpdateTranslator(const terrier::planner::UpdatePlanNode *op, CodeGen *codegen)
    : OperatorTranslator(codegen, brain::ExecutionOperatingUnitType::UPDATE),
      op_(op),
      updater_(codegen->NewIdentifier("updater")),
      update_pr_(codegen->NewIdentifier("update_pr")),
      col_oids_(codegen->NewIdentifier("col_oids")),
      table_schema_(codegen->Accessor()->GetSchema(op_->GetTableOid())),
      all_oids_(CollectOids(op)),
      table_pm_(codegen->Accessor()->GetTable(op_->GetTableOid())->ProjectionMapForOids(all_oids_)),
      pr_filler_(codegen_, table_schema_, table_pm_, update_pr_) {}

void UpdateTranslator::Produce(FunctionBuilder *builder) {
  DeclareUpdater(builder);
  child_translator_->Produce(builder);
  GenUpdaterFree(builder);
}

void UpdateTranslator::Abort(FunctionBuilder *builder) {
  GenUpdaterFree(builder);
  child_translator_->Abort(builder);
  builder->Append(codegen_->ReturnStmt(nullptr));
}

void UpdateTranslator::Consume(FunctionBuilder *builder) {
  DeclareUpdatePR(builder);
  if (op_->GetIndexedUpdate()) {
    // For indexed updates, we need to call delete first
    GenTableDelete(builder);
  }
  GetUpdatePR(builder);
  FillPRFromChild(builder);

  if (op_->GetIndexedUpdate()) {
    // Indexed updates re-insert into the table
    GenTableInsert(builder);
    // Then they delete and insert into every index.
    // Update into every index
    const auto &indexes = codegen_->Accessor()->GetIndexOids(op_->GetTableOid());
    for (auto &index_oid : indexes) {
      GenIndexDelete(builder, index_oid);
      GenIndexInsert(builder, index_oid);
    }
    return;
  }
  // Non indexed updates just update.
  GenTableUpdate(builder);
}

void UpdateTranslator::DeclareUpdater(terrier::execution::compiler::FunctionBuilder *builder) {
  // Generate col oids
  SetOids(builder);
  // var updater : StorageInterface
  auto storage_interface_type = codegen_->BuiltinType(ast::BuiltinType::Kind::StorageInterface);
  builder->Append(codegen_->DeclareVariable(updater_, storage_interface_type, nullptr));
  // Call @storageInterfaceInit
  ast::Expr *updater_setup = codegen_->StorageInterfaceInit(updater_, !op_->GetTableOid(), col_oids_, true);
  builder->Append(codegen_->MakeStmt(updater_setup));
}

void UpdateTranslator::GenUpdaterFree(terrier::execution::compiler::FunctionBuilder *builder) {
  // Call @storageInterfaceFree
  ast::Expr *updater_free = codegen_->OneArgCall(ast::Builtin::StorageInterfaceFree, updater_, true);
  builder->Append(codegen_->MakeStmt(updater_free));
}

ast::Expr *UpdateTranslator::GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) {
  TERRIER_ASSERT(child_idx == 0, "Update plan can only have one child");
  return child_translator_->GetOutput(attr_idx);
}

ast::Expr *UpdateTranslator::GetTableColumn(const catalog::col_oid_t &col_oid) {
  // TODO(Amadou): This relies on the fact that update plan nodes come after table or index scans.
  // If that turns out to not be the case, then update plans should use DVEs instead of CVEs.
  return child_translator_->GetTableColumn(col_oid);
}

void UpdateTranslator::SetOids(FunctionBuilder *builder) {
  // Declare: var col_oids: [num_cols]uint32
  ast::Expr *arr_type = codegen_->ArrayType(all_oids_.size(), ast::BuiltinType::Kind::Uint32);
  builder->Append(codegen_->DeclareVariable(col_oids_, arr_type, nullptr));

  // For each oid, set col_oids[i] = col_oid
  for (uint16_t i = 0; i < all_oids_.size(); i++) {
    ast::Expr *lhs = codegen_->ArrayAccess(col_oids_, i);
    ast::Expr *rhs = codegen_->IntLiteral(!all_oids_[i]);
    builder->Append(codegen_->Assign(lhs, rhs));
  }
}

void UpdateTranslator::DeclareUpdatePR(terrier::execution::compiler::FunctionBuilder *builder) {
  // var update_pr : *ProjectedRow
  auto pr_type = codegen_->BuiltinType(ast::BuiltinType::Kind::ProjectedRow);
  builder->Append(codegen_->DeclareVariable(update_pr_, codegen_->PointerType(pr_type), nullptr));
}

void UpdateTranslator::GetUpdatePR(terrier::execution::compiler::FunctionBuilder *builder) {
  // var update_pr = ProjectedRow
  auto get_pr_call = codegen_->OneArgCall(ast::Builtin::GetTablePR, updater_, true);
  builder->Append(codegen_->Assign(codegen_->MakeExpr(update_pr_), get_pr_call));
}

void UpdateTranslator::FillPRFromChild(terrier::execution::compiler::FunctionBuilder *builder) {
  const auto &clauses = op_->GetSetClauses();

  for (const auto &clause : clauses) {
    const auto &table_col_oid = clause.first;
    const auto &table_col = table_schema_.GetColumn(table_col_oid);
    auto translator = TranslatorFactory::CreateExpressionTranslator(clause.second.Get(), codegen_);
    auto clause_expr = translator->DeriveExpr(this);
    auto pr_set_call = codegen_->PRSet(codegen_->MakeExpr(update_pr_), table_col.Type(), table_col.Nullable(),
                                       table_pm_[table_col_oid], clause_expr, true);
    builder->Append(codegen_->MakeStmt(pr_set_call));
  }
}

void UpdateTranslator::GenTableUpdate(FunctionBuilder *builder) {
  //   if (update fails) { Abort(); }
  auto update_slot = child_translator_->GetSlot();
  std::vector<ast::Expr *> update_args{codegen_->PointerTo(updater_), update_slot};
  auto update_call = codegen_->BuiltinCall(ast::Builtin::TableUpdate, std::move(update_args));

  auto cond = codegen_->UnaryOp(parsing::Token::Type::BANG, update_call);
  builder->StartIfStmt(cond);
  Abort(builder);
  builder->FinishBlockStmt();
}

void UpdateTranslator::GenTableInsert(FunctionBuilder *builder) {
  // var insert_slot = @tableInsert(&updater_)
  auto insert_slot = codegen_->NewIdentifier("insert_slot");
  auto insert_call = codegen_->OneArgCall(ast::Builtin::TableInsert, updater_, true);

  builder->Append(codegen_->DeclareVariable(insert_slot, nullptr, insert_call));
}

void UpdateTranslator::GenIndexInsert(FunctionBuilder *builder, const catalog::index_oid_t &index_oid) {
  // var insert_index_pr = @getIndexPR(&inserter, oid)
  auto insert_index_pr = codegen_->NewIdentifier("insert_index_pr");
  std::vector<ast::Expr *> pr_call_args{codegen_->PointerTo(updater_), codegen_->IntLiteral(!index_oid)};
  auto get_index_pr_call = codegen_->BuiltinCall(ast::Builtin::GetIndexPR, std::move(pr_call_args));
  builder->Append(codegen_->DeclareVariable(insert_index_pr, nullptr, get_index_pr_call));

  // Fill up the index pr from the update_pr
  auto index = codegen_->Accessor()->GetIndex(index_oid);
  const auto &index_pm = index->GetKeyOidToOffsetMap();
  const auto &index_schema = codegen_->Accessor()->GetIndexSchema(index_oid);

  pr_filler_.GenFiller(index_pm, index_schema, codegen_->MakeExpr(insert_index_pr), builder);

  // Insert into index
  // if (insert not successfull) { Abort(); }
  auto index_insert_call = codegen_->OneArgCall(
      index_schema.Unique() ? ast::Builtin::IndexInsertUnique : ast::Builtin::IndexInsert, updater_, true);
  auto cond = codegen_->UnaryOp(parsing::Token::Type::BANG, index_insert_call);
  builder->StartIfStmt(cond);
  Abort(builder);
  builder->FinishBlockStmt();
}

void UpdateTranslator::GenTableDelete(FunctionBuilder *builder) {
  // Delete from table
  // if (delete not successfull) { Abort(); }
  auto delete_slot = child_translator_->GetSlot();
  std::vector<ast::Expr *> delete_args{codegen_->PointerTo(updater_), delete_slot};
  auto delete_call = codegen_->BuiltinCall(ast::Builtin::TableDelete, std::move(delete_args));
  auto cond = codegen_->UnaryOp(parsing::Token::Type::BANG, delete_call);
  builder->StartIfStmt(cond);
  Abort(builder);
  builder->FinishBlockStmt();
}

void UpdateTranslator::GenIndexDelete(FunctionBuilder *builder, const catalog::index_oid_t &index_oid) {
  // var delete_index_pr = @getIndexPR(&deleter, oid)
  auto delete_index_pr = codegen_->NewIdentifier("delete_index_pr");
  std::vector<ast::Expr *> pr_call_args{codegen_->PointerTo(updater_), codegen_->IntLiteral(!index_oid)};
  auto get_index_pr_call = codegen_->BuiltinCall(ast::Builtin::GetIndexPR, std::move(pr_call_args));

  builder->Append(codegen_->DeclareVariable(delete_index_pr, nullptr, get_index_pr_call));

  // Fill up the index pr
  auto index = codegen_->Accessor()->GetIndex(index_oid);
  const auto &index_pm = index->GetKeyOidToOffsetMap();
  const auto &index_schema = codegen_->Accessor()->GetIndexSchema(index_oid);
  const auto &index_cols = index_schema.GetColumns();
  for (const auto &index_col : index_cols) {
    // NOTE: index expressions in delete refer to columns in the child translator (before any update is done).
    // For example, if the child is a seq scan, the index expressions could contain ColumnValueExpressions.
    auto translator = TranslatorFactory::CreateExpressionTranslator(index_col.StoredExpression().Get(), codegen_);
    auto val = translator->DeriveExpr(child_translator_);
    auto pr_set_call = codegen_->PRSet(codegen_->MakeExpr(delete_index_pr), index_col.Type(), index_col.Nullable(),
                                       index_pm.at(index_col.Oid()), val);
    builder->Append(codegen_->MakeStmt(pr_set_call));
  }

  // Delete from index
  std::vector<ast::Expr *> delete_args{codegen_->PointerTo(updater_), child_translator_->GetSlot()};
  auto index_delete_call = codegen_->BuiltinCall(ast::Builtin::IndexDelete, std::move(delete_args));
  builder->Append(codegen_->MakeStmt(index_delete_call));
}

}  // namespace terrier::execution::compiler
