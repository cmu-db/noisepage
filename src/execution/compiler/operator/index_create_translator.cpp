#include "execution/compiler/operator/index_create_translator.h"

#include <utility>
#include <vector>
#include "catalog/catalog_accessor.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/translator_factory.h"

namespace terrier::execution::compiler {
// TODO(Wuwen): not sure what is correct for ExecutionOperatingUnitType
CreateIndexTranslator::CreateIndexTranslator(const terrier::planner::CreateIndexPlanNode *op, CodeGen *codegen)
    : OperatorTranslator(codegen, brain::ExecutionOperatingUnitType::CREATE_INDEX),
      op_(op),
      index_inserter_(codegen_->NewIdentifier("index_inserter")),
      col_oids_(codegen_->NewIdentifier("col_oids")),
      index_oid_(codegen_->NewIdentifier("index_oid")),
      tvi_(codegen_->NewIdentifier("tvi")),
      pci_(codegen_->NewIdentifier("pci")),
      table_schema_(codegen_->Accessor()->GetSchema(op_->GetTableOid())),
      all_oids_(AllColOids(table_schema_)) {}


void CreateIndexTranslator::Produce(FunctionBuilder *builder) {
  // Generate col oids
  SetOids(builder);
  DeclareIndexInserter(builder);
  // get table iterator
  DeclareTVI(builder);
  // create index
  GenCreateIndex(builder);
  // get index pr
  DeclareIndexPR(builder);
  // begin loop
  GenTVILoop(builder);
  // get table pr
  DeclarePCI(builder);

  GenPCILoop(builder);

  DeclareSlot(builder);

  DeclareTablePR(builder);
  // insert
  GenIndexInsert(builder);
  // reset
  GenTVIReset(builder);
  // Close PCI loop
  builder->FinishBlockStmt();
  // Close TVI loop
  builder->FinishBlockStmt();
  // clean up
  FreeIterator(builder);
  GenIndexInserterFree(builder);
}

void CreateIndexTranslator::Abort(FunctionBuilder *builder) {
  GenIndexInserterFree(builder);
  if (child_translator_ != nullptr) child_translator_->Abort(builder);
  builder->Append(codegen_->ReturnStmt(nullptr));
}

void CreateIndexTranslator::Consume(FunctionBuilder *builder) {

}

void CreateIndexTranslator::DeclareIndexInserter(FunctionBuilder *builder) {
  // var index_inserter : StorageInterface
  auto storage_interface_type = codegen_->BuiltinType(ast::BuiltinType::Kind::StorageInterface);
  builder->Append(codegen_->DeclareVariable(index_inserter_, storage_interface_type, nullptr));
  // Call @storageInterfaceInit
  ast::Expr *index_inserter_setup = codegen_->StorageInterfaceInit(index_inserter_, !op_->GetTableOid(), col_oids_, false);
  builder->Append(codegen_->MakeStmt(index_inserter_setup));
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

//void CreateIndexTranslator::GenTVIReset(execution::compiler::FunctionBuilder *builder) {
//  // Reset iterator
//  ast::Expr *reset_call = codegen_->OneArgCall(ast::Builtin::TableIterReset, tvi_, true);
//  builder->Append(codegen_->MakeStmt(reset_call));
//}

void CreateIndexTranslator::GenCreateIndex(FunctionBuilder *builder) {
  auto index_oid = codegen_->Accessor()->CreateIndex(op_->GetNamespaceOid(), op_->GetTableOid(), op_->GetIndexName(), *(op_->GetSchema()));
  // TODO(Wuwen): check if index_oid is valid
  const auto &index_schema = codegen_->Accessor()->GetIndexSchema(index_oid);

  storage::index::IndexBuilder index_builder;
  index_builder.SetKeySchema(index_schema);
  auto *const index = index_builder.Build();
  bool result UNUSED_ATTRIBUTE = codegen_->Accessor()->SetIndexPointer(index_oid, index);

  ast::Expr * index_oid_expr_ = codegen_->IntLiteral(!index_oid);
  codegen_->DeclareVariable(index_oid_, index_oid_expr_, nullptr);
}

void CreateIndexTranslator::DeclareIndexPR(FunctionBuilder *builder) {

}

void CreateIndexTranslator::GenIndexInserterFree(terrier::execution::compiler::FunctionBuilder *builder) {
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

ast::Expr *CreateIndexTranslator::GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) {
  TERRIER_ASSERT(child_idx == 0, "Insert plan can only have one child");
  return child_translator_->GetOutput(attr_idx);
}

std::vector<catalog::col_oid_t> CreateIndexTranslator::AllColOids(const catalog::Schema &table_schema_) {
  std::vector<catalog::col_oid_t> oids;
  for (const auto &col : table_schema_.GetColumns()) {
    oids.emplace_back(col.Oid());
  }
  return oids;
}
}  // namespace terrier::execution::compiler
