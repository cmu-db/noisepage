#include "execution/compiler/operator/index_create_translator.h"

#include <utility>
#include <vector>

#include "execution/compiler/function_builder.h"
#include "execution/compiler/translator_factory.h"

namespace terrier::execution::compiler {
// TODO(Wuwen): not sure what is correct for ExecutionOperatingUnitType
CreateIndexTranslator::CreateIndexTranslator(const terrier::planner::CreateIndexPlanNode *op, CodeGen *codegen)
    : OperatorTranslator(codegen, brain::ExecutionOperatingUnitType::CREATE_INDEX),
      op_(op),
      index_inserter_(codegen->NewIdentifier("index_inserter")),
      table_schema_(codegen->Accessor()->GetSchema(op_->GetTableOid())),
      all_oids_(AllColOids(table_schema_)),
      col_oids_(codegen->NewIdentifier("col_oids")),{}

void CreateIndexTranslator::Produce(FunctionBuilder *builder) {
  DeclareIndexInserter(builder);

  GenCreateIndex(builder);

  GenIndexInserterFree(builder);
}

void CreateIndexTranslator::Abort(FunctionBuilder *builder) {
  GenIndexInserterFree(builder);
  if (child_translator_ != nullptr) child_translator_->Abort(builder);
  builder->Append(codegen_->ReturnStmt(nullptr));
}

void CreateIndexTranslator::Consume(FunctionBuilder *builder) {

}

void CreateIndexTranslator::DeclareIndexInserter(terrier::execution::compiler::FunctionBuilder *builder) {
  // Generate col oids
  SetOids(builder);
  // var index_inserter : StorageInterface
  auto storage_interface_type = codegen_->BuiltinType(ast::BuiltinType::Kind::StorageInterface);
  builder->Append(codegen_->DeclareVariable(index_inserter_, storage_interface_type, nullptr));
  // Call @storageInterfaceInit
  ast::Expr *index_inserter_setup = codegen_->StorageInterfaceInit(index_inserter_, !op_->GetTableOid(), col_oids_, true);
  builder->Append(codegen_->MakeStmt(index_inserter_setup));
}

void CreateIndexTranslator::GenIndexInserterFree(terrier::execution::compiler::FunctionBuilder *builder) {
  // Call @storageInterfaceFree
  ast::Expr *index_inserter_free = codegen_->OneArgCall(ast::Builtin::StorageInterfaceFree, index_inserter_, true);
  builder->Append(codegen_->MakeStmt(index_inserter_free));
}

void CreateIndexTranslator::GenCreateIndex(FunctionBuilder *builder) {
  uint32_t index_oid = codegen_->Accessor()->CreateIndex(op_->GetNamespaceOid(), op_->GetTableOid(), op_->GetIndexName(), op_->GetSchema());
  // TODO(Wuwen): check if index_oid is valid
  std::vector<ast::Expr *> build_args{codegen_->PointerTo(index_inserter_), index_oid};
  auto index_insert_call = codegen_->BuiltinCall(ast::Builtin::IndexCreate, std::move(build_args));
  auto cond = codegen_->UnaryOp(parsing::Token::Type::BANG, index_insert_call);
  builder->StartIfStmt(cond);
  Abort(builder);
  builder->FinishBlockStmt();
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
}  // namespace terrier::execution::compiler
