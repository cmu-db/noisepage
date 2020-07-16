#include "execution/compiler/operator/index_create_translator.h"

#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/translator_factory.h"
#include "execution/sql/ddl_executors.h"
#include "planner/plannodes/create_index_plan_node.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"
#include "storage/storage_util.h"

namespace terrier::execution::compiler {
// TODO(Wuwen): not sure what is correct for ExecutionOperatingUnitType
CreateIndexTranslator::CreateIndexTranslator(const terrier::planner::CreateIndexPlanNode *op, CodeGen *codegen)
    : OperatorTranslator(codegen, brain::ExecutionOperatingUnitType::CREATE_INDEX),
      op_(op),
      index_inserter_(codegen_->NewIdentifier("index_inserter")),
      index_pr_(codegen_->NewIdentifier("index_pr")),
      table_pr_(codegen_->NewIdentifier("table_pr")),
      col_oids_(codegen_->NewIdentifier("col_oids")),
      slot_(codegen_->NewIdentifier("slot")),
      table_schema_(codegen_->Accessor()->GetSchema(op_->GetTableOid())),
      index_oid_(),
      all_oids_(storage::StorageUtil::AllColOids(table_schema_)),
      table_pm_(codegen_->Accessor()->GetTable(op_->GetTableOid())->ProjectionMapForOids(all_oids_)),
      pr_filler_(codegen_, table_schema_, table_pm_, table_pr_),
      seq_scanner_(codegen_) {}

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
  builder->Append(codegen_->MakeStmt(init_table_pr_call));
}

void CreateIndexTranslator::GenFillTablePR(FunctionBuilder *builder) {
  std::vector<ast::Expr *> insert_args{codegen_->PointerTo(index_inserter_), codegen_->PointerTo(slot_)};
  auto fill_pr_call = codegen_->BuiltinCall(ast::Builtin::FillTablePR, std::move(insert_args));
  builder->Append(codegen_->DeclareVariable(table_pr_, nullptr, fill_pr_call));
}

void CreateIndexTranslator::GenIndexInsert(FunctionBuilder *builder) {
  // Fill up the index pr
  auto index = codegen_->Accessor()->GetIndex(index_oid_);
  const auto &index_pm = index->GetKeyOidToOffsetMap();
  const auto &index_schema = codegen_->Accessor()->GetIndexSchema(index_oid_);

  pr_filler_.GenFiller(index_pm, index_schema, codegen_->MakeExpr(index_pr_), builder);

  auto index_insert_call = codegen_->OneArgCall(
      index_schema.Unique() ? ast::Builtin::IndexInsertUnique : ast::Builtin::IndexInsert, index_inserter_, true);
  auto cond = codegen_->UnaryOp(parsing::Token::Type::BANG, index_insert_call);
  builder->StartIfStmt(cond);
  Abort(builder);
  builder->FinishBlockStmt();
}

void CreateIndexTranslator::DeclareTVI(FunctionBuilder *builder) {
  // Create TableVectorIterator and init
  seq_scanner_.DeclareTVI(builder, !op_->GetTableOid(), col_oids_);
}

void CreateIndexTranslator::GenTVILoop(FunctionBuilder *builder) {
  // The advance call of TVI
  seq_scanner_.TVILoop(builder);
}

void CreateIndexTranslator::DeclarePCI(FunctionBuilder *builder) {
  // Create ProjectedColumnIterator and init
  seq_scanner_.DeclarePCI(builder);
}

void CreateIndexTranslator::DeclareSlot(FunctionBuilder *builder) {
  // Get tuple slot
  slot_ = seq_scanner_.DeclareSlot(builder);
}

void CreateIndexTranslator::GenPCILoop(FunctionBuilder *builder) {
  // The advance call of PCI
  seq_scanner_.PCILoop(builder);
}

void CreateIndexTranslator::GenTVIClose(FunctionBuilder *builder) {
  // Close Iterator
  seq_scanner_.TVIClose(builder);
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

const planner::AbstractPlanNode *CreateIndexTranslator::Op() { return op_; }

}  // namespace terrier::execution::compiler
