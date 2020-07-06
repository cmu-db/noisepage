#include "execution/compiler/operator/index_create_translator.h"

#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/translator_factory.h"
#include "execution/sql/ddl_executors.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"

namespace terrier::execution::compiler {
// TODO(Wuwen): not sure what is correct for ExecutionOperatingUnitType
CreateIndexTranslator::CreateIndexTranslator(const terrier::planner::CreateIndexPlanNode *op, CodeGen *codegen)
    : OperatorTranslator(codegen, brain::ExecutionOperatingUnitType::CREATE_INDEX),
      op_(op),
      index_inserter_(codegen_->NewIdentifier("index_inserter")),
      index_pr_(codegen_->NewIdentifier("index_pr")),
      table_pr_(codegen_->NewIdentifier("table_pr")),
      col_oids_(codegen_->NewIdentifier("col_oids")),
      tvi_(codegen_->NewIdentifier("tvi")),
      pci_(codegen_->NewIdentifier("pci")),
      slot_(codegen_->NewIdentifier("slot")),
      table_schema_(codegen_->Accessor()->GetSchema(op_->GetTableOid())),
      index_oid_(),
      all_oids_(AllColOids(table_schema_)),
      table_pm_(codegen_->Accessor()->GetTable(op_->GetTableOid())->ProjectionMapForOids(all_oids_)),
      pr_filler_(codegen_, table_schema_, table_pm_, index_inserter_) {}

void CreateIndexTranslator::Produce(FunctionBuilder *builder) {
  // Generate col oids
  SetOids(builder);
  // Init storageInterface
  DeclareIndexInserter(builder);
  GenCreateIndex(builder);
  GenGetIndexPR(builder);
  GenGetTablePR(builder);

  // Table iterator
  DeclareTVI(builder);
  // TVI loop
  GenTVILoop(builder);
  // PC iterator
  DeclarePCI(builder);
  // PCI loop
  GenPCILoop(builder);
  // Get slot and fill table pr
  DeclareSlot(builder);
  GenFillTablePR(builder);
  // Insert into index
  GenIndexInsert(builder);
  // Close PCI loop
  builder->FinishBlockStmt();
  // Close TVI loop
  builder->FinishBlockStmt();
  // clean up
  GenTVIClose(builder);
  GenIndexInserterFree(builder);
}

void CreateIndexTranslator::Abort(FunctionBuilder *builder) {
  GenIndexInserterFree(builder);
  if (child_translator_ != nullptr) child_translator_->Abort(builder);
  builder->Append(codegen_->ReturnStmt(nullptr));
}

void CreateIndexTranslator::Consume(FunctionBuilder *builder) {}

void CreateIndexTranslator::DeclareIndexInserter(FunctionBuilder *builder) {
  // var index_inserter : StorageInterface
  auto storage_interface_type = codegen_->BuiltinType(ast::BuiltinType::Kind::StorageInterface);
  builder->Append(codegen_->DeclareVariable(index_inserter_, storage_interface_type, nullptr));
  // Call @storageInterfaceInit
  ast::Expr *index_inserter_setup =
      codegen_->StorageInterfaceInit(index_inserter_, !op_->GetTableOid(), col_oids_, false);
  builder->Append(codegen_->MakeStmt(index_inserter_setup));
}

void CreateIndexTranslator::GenCreateIndex(FunctionBuilder *builder) {
  auto plan_node = common::ManagedPointer<const planner::CreateIndexPlanNode>(op_);
  auto result = sql::DDLExecutors::CreateIndexExecutor(
      plan_node, common::ManagedPointer<catalog::CatalogAccessor>(codegen_->Accessor()));
  if (!result) {
    Abort(builder);
  }
  index_oid_ = codegen_->Accessor()->GetIndexOid(op_->GetIndexName());
}

void CreateIndexTranslator::GenGetIndexPR(FunctionBuilder *builder) {
  // var insert_index_pr = @getIndexPR(&inserter, oid)
  std::vector<ast::Expr *> pr_call_args{codegen_->PointerTo(index_inserter_), codegen_->IntLiteral(!index_oid_)};
  auto get_index_pr_call = codegen_->BuiltinCall(ast::Builtin::GetIndexPR, std::move(pr_call_args));
  builder->Append(codegen_->DeclareVariable(index_pr_, nullptr, get_index_pr_call));
}

void CreateIndexTranslator::GenGetTablePR(FunctionBuilder *builder) {
  // var table_pr = @InitTablePR()
  std::vector<ast::Expr *> pr_call_args{codegen_->PointerTo(index_inserter_), codegen_->IntLiteral(!index_oid_)};
  auto init_table_pr_call = codegen_->BuiltinCall(ast::Builtin::InitTablePR, std::move(pr_call_args));
  builder->Append(codegen_->DeclareVariable(table_pr_, nullptr, init_table_pr_call));
}

void CreateIndexTranslator::GenFillTablePR(FunctionBuilder *builder) {
  std::vector<ast::Expr *> insert_args{codegen_->PointerTo(index_inserter_), codegen_->PointerTo(slot_)};
  auto fill_pr_call = codegen_->BuiltinCall(ast::Builtin::FillTablePR, std::move(insert_args));
  builder->Append(codegen_->MakeStmt(fill_pr_call));
}

// TODO(Wuwen): find out if GenFiller could work in this case
void CreateIndexTranslator::GenIndexInsert(FunctionBuilder *builder) {
  const auto &index_schema = codegen_->Accessor()->GetIndexSchema(index_oid_);

  auto index_insert_call = codegen_->OneArgCall(
      index_schema.Unique() ? ast::Builtin::IndexInsertUnique : ast::Builtin::IndexInsert, index_inserter_, true);
  auto cond = codegen_->UnaryOp(parsing::Token::Type::BANG, index_insert_call);
  builder->StartIfStmt(cond);
  Abort(builder);
  builder->FinishBlockStmt();
}

void CreateIndexTranslator::DeclareTVI(FunctionBuilder *builder) {
  // Declare local var table_iter
  ast::Expr *iter_type = codegen_->BuiltinType(ast::BuiltinType::TableVectorIterator);
  builder->Append(codegen_->DeclareVariable(tvi_, iter_type, nullptr));

  // // Call @tableIterInit(&tvi, execCtx, table_oid, col_oids)
  ast::Expr *init_call = codegen_->TableIterInit(tvi_, !op_->GetTableOid(), col_oids_);
  builder->Append(codegen_->MakeStmt(init_call));
}

void CreateIndexTranslator::GenTVILoop(FunctionBuilder *builder) {
  // The advance call
  ast::Expr *advance_call = codegen_->OneArgCall(ast::Builtin::TableIterAdvance, tvi_, true);
  builder->StartForStmt(nullptr, advance_call, nullptr);
}

void CreateIndexTranslator::DeclarePCI(FunctionBuilder *builder) {
  // Assign var pci = @tableIterGetPCI(&tvi)
  ast::Expr *get_pci_call = codegen_->OneArgCall(ast::Builtin::TableIterGetPCI, tvi_, true);
  builder->Append(codegen_->DeclareVariable(pci_, nullptr, get_pci_call));
}

void CreateIndexTranslator::DeclareSlot(FunctionBuilder *builder) {
  // Get var slot = @pciGetSlot(pci)
  ast::Expr *get_slot_call = codegen_->OneArgCall(ast::Builtin::PCIGetSlot, pci_, false);
  builder->Append(codegen_->DeclareVariable(slot_, nullptr, get_slot_call));
}

void CreateIndexTranslator::GenPCILoop(FunctionBuilder *builder) {
  // Generate for(; @pciHasNext(pci); @pciAdvance(pci)) {...} or the Filtered version
  // The HasNext call
  ast::Expr *has_next_call = codegen_->OneArgCall(ast::Builtin::PCIHasNext, pci_, false);
  // The Advance call
  ast::Expr *advance_call = codegen_->OneArgCall(ast::Builtin::PCIAdvance, pci_, false);
  ast::Stmt *loop_advance = codegen_->MakeStmt(advance_call);
  // Make the for loop.
  builder->StartForStmt(nullptr, has_next_call, loop_advance);
}

void CreateIndexTranslator::GenTVIClose(FunctionBuilder *builder) {
  // Close iterator
  ast::Expr *close_call = codegen_->OneArgCall(ast::Builtin::TableIterClose, tvi_, true);
  builder->Append(codegen_->MakeStmt(close_call));
}

void CreateIndexTranslator::GenIndexInserterFree(FunctionBuilder *builder) {
  // Call @storageInterfaceFree
  ast::Expr *index_inserter_free = codegen_->OneArgCall(ast::Builtin::StorageInterfaceFree, index_inserter_, true);
  builder->Append(codegen_->MakeStmt(index_inserter_free));
}

void CreateIndexTranslator::SetOids(FunctionBuilder *builder) {
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

std::vector<catalog::col_oid_t> CreateIndexTranslator::AllColOids(const catalog::Schema &table_schema_) {
  std::vector<catalog::col_oid_t> oids;
  for (const auto &col : table_schema_.GetColumns()) {
    oids.emplace_back(col.Oid());
  }
  return oids;
}

}  // namespace terrier::execution::compiler
