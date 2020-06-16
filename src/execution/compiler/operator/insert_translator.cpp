#include "execution/compiler/operator/insert_translator.h"

#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/translator_factory.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"

namespace terrier::execution::compiler {
InsertTranslator::InsertTranslator(const terrier::planner::InsertPlanNode *op, CodeGen *codegen)
    : OperatorTranslator(codegen, brain::ExecutionOperatingUnitType::INSERT),
      op_(op),
      inserter_(codegen->NewIdentifier("inserter")),
      insert_pr_(codegen->NewIdentifier("insert_pr")),
      col_oids_(codegen->NewIdentifier("col_oids")),
      table_schema_(codegen->Accessor()->GetSchema(op_->GetTableOid())),
      all_oids_(AllColOids(table_schema_)),
      table_pm_(codegen->Accessor()->GetTable(op_->GetTableOid())->ProjectionMapForOids(all_oids_)),
      pr_filler_(codegen_, table_schema_, table_pm_, insert_pr_) {}

void InsertTranslator::Produce(FunctionBuilder *builder) {
  DeclareInserter(builder);

  if (op_->GetChildrenSize() != 0) {
    // This is an insert into select so let children produce
    child_translator_->Produce(builder);
    GenInserterFree(builder);
    return;
  }

  // Otherwise, this is a raw insert.
  DeclareInsertPR(builder);
  // For each set of values, insert into table and indexes
  for (uint32_t idx = 0; idx < op_->GetBulkInsertCount(); idx++) {
    // Get the table PR
    GetInsertPR(builder);
    // Set the table PR
    GenSetTablePR(builder, idx);
    // Insert into Table
    GenTableInsert(builder);
    // Insert into each index.
    const auto &indexes = codegen_->Accessor()->GetIndexOids(op_->GetTableOid());
    for (auto &index_oid : indexes) {
      GenIndexInsert(builder, index_oid);
    }
  }
  GenInserterFree(builder);
}

void InsertTranslator::Abort(FunctionBuilder *builder) {
  GenInserterFree(builder);
  if (child_translator_ != nullptr) child_translator_->Abort(builder);
  builder->Append(codegen_->ReturnStmt(nullptr));
}

void InsertTranslator::Consume(FunctionBuilder *builder) {
  // Declare & Get table PR
  DeclareInsertPR(builder);
  GetInsertPR(builder);

  // Set the values to insert
  FillPRFromChild(builder);

  // Insert into table
  GenTableInsert(builder);

  // Insert into every index
  const auto &indexes = codegen_->Accessor()->GetIndexOids(op_->GetTableOid());
  for (auto &index_oid : indexes) {
    GenIndexInsert(builder, index_oid);
  }
}

void InsertTranslator::DeclareInserter(terrier::execution::compiler::FunctionBuilder *builder) {
  // Generate col oids
  SetOids(builder);
  // var inserter : StorageInterface
  auto storage_interface_type = codegen_->BuiltinType(ast::BuiltinType::Kind::StorageInterface);
  builder->Append(codegen_->DeclareVariable(inserter_, storage_interface_type, nullptr));
  // Call @storageInterfaceInit
  ast::Expr *inserter_setup = codegen_->StorageInterfaceInit(inserter_, !op_->GetTableOid(), col_oids_, true);
  builder->Append(codegen_->MakeStmt(inserter_setup));
}

void InsertTranslator::GenInserterFree(terrier::execution::compiler::FunctionBuilder *builder) {
  // Call @storageInterfaceFree
  ast::Expr *inserter_free = codegen_->OneArgCall(ast::Builtin::StorageInterfaceFree, inserter_, true);
  builder->Append(codegen_->MakeStmt(inserter_free));
}

ast::Expr *InsertTranslator::GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) {
  TERRIER_ASSERT(child_idx == 0, "Insert plan can only have one child");
  return child_translator_->GetOutput(attr_idx);
}

void InsertTranslator::SetOids(FunctionBuilder *builder) {
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

void InsertTranslator::DeclareInsertPR(terrier::execution::compiler::FunctionBuilder *builder) {
  // var insert_pr : *ProjectedRow
  auto pr_type = codegen_->BuiltinType(ast::BuiltinType::Kind::ProjectedRow);
  builder->Append(codegen_->DeclareVariable(insert_pr_, codegen_->PointerType(pr_type), nullptr));
}

void InsertTranslator::GetInsertPR(terrier::execution::compiler::FunctionBuilder *builder) {
  // var insert_pr = getTablePR(...)
  auto get_pr_call = codegen_->OneArgCall(ast::Builtin::GetTablePR, inserter_, true);
  builder->Append(codegen_->Assign(codegen_->MakeExpr(insert_pr_), get_pr_call));
}

void InsertTranslator::GenSetTablePR(FunctionBuilder *builder, uint32_t idx) {
  const auto &node_vals = op_->GetValues(idx);
  for (size_t i = 0; i < node_vals.size(); i++) {
    auto &val = node_vals[i];
    auto translator = TranslatorFactory::CreateExpressionTranslator(val.Get(), codegen_);

    auto *src = translator->DeriveExpr(this);
    auto table_col_oid = all_oids_[i];
    const auto &table_col = table_schema_.GetColumn(table_col_oid);
    auto pr_set_call = codegen_->PRSet(codegen_->MakeExpr(insert_pr_), table_col.Type(), table_col.Nullable(),
                                       table_pm_[table_col_oid], src, true);
    builder->Append(codegen_->MakeStmt(pr_set_call));
  }
}

void InsertTranslator::GenTableInsert(FunctionBuilder *builder) {
  // var insert_slot = @tableInsert(&inserter_)
  auto insert_slot = codegen_->NewIdentifier("insert_slot");
  auto insert_call = codegen_->OneArgCall(ast::Builtin::TableInsert, inserter_, true);
  builder->Append(codegen_->DeclareVariable(insert_slot, nullptr, insert_call));
}

void InsertTranslator::GenIndexInsert(FunctionBuilder *builder, const catalog::index_oid_t &index_oid) {
  // var insert_index_pr = @getIndexPR(&inserter, oid)
  auto insert_index_pr = codegen_->NewIdentifier("insert_index_pr");
  std::vector<ast::Expr *> pr_call_args{codegen_->PointerTo(inserter_), codegen_->IntLiteral(!index_oid)};
  auto get_index_pr_call = codegen_->BuiltinCall(ast::Builtin::GetIndexPR, std::move(pr_call_args));
  builder->Append(codegen_->DeclareVariable(insert_index_pr, nullptr, get_index_pr_call));

  // Fill up the index pr
  auto index = codegen_->Accessor()->GetIndex(index_oid);
  const auto &index_pm = index->GetKeyOidToOffsetMap();
  const auto &index_schema = codegen_->Accessor()->GetIndexSchema(index_oid);

  pr_filler_.GenFiller(index_pm, index_schema, codegen_->MakeExpr(insert_index_pr), builder);

  // Insert into index
  // if (insert not successfull) { Abort(); }
  auto index_insert_call = codegen_->OneArgCall(
      index_schema.Unique() ? ast::Builtin::IndexInsertUnique : ast::Builtin::IndexInsert, inserter_, true);
  auto cond = codegen_->UnaryOp(parsing::Token::Type::BANG, index_insert_call);
  builder->StartIfStmt(cond);
  Abort(builder);
  builder->FinishBlockStmt();
}

void InsertTranslator::FillPRFromChild(terrier::execution::compiler::FunctionBuilder *builder) {
  const auto &cols = table_schema_.GetColumns();

  for (uint32_t i = 0; i < all_oids_.size(); i++) {
    const auto &table_col = cols[i];
    const auto &table_col_oid = all_oids_[i];
    auto val = GetChildOutput(0, i, table_col.Type());
    auto pr_set_call = codegen_->PRSet(codegen_->MakeExpr(insert_pr_), table_col.Type(), table_col.Nullable(),
                                       table_pm_[table_col_oid], val, true);
    builder->Append(codegen_->MakeStmt(pr_set_call));
  }
}

}  // namespace terrier::execution::compiler
