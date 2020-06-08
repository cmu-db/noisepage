#include "execution/compiler/operator/create_index_translator.h"

#include <utility>
#include <vector>

#include "execution/compiler/function_builder.h"
#include "execution/compiler/translator_factory.h"

namespace terrier::execution::compiler {
// TODO(Wuwen): not sure what is correct for ExecutionOperatingUnitType
CreateIndexTranslator::CreateIndexTranslator(const terrier::planner::CreateIndexPlanNode *op, CodeGen *codegen)
    : OperatorTranslator(codegen, brain::ExecutionOperatingUnitType::INSERT),
      op_(op),
      index_inserter_(codegen->NewIdentifier("index_inserter")),
      col_oids_(codegen->NewIdentifier("col_oids")),{}

void CreateIndexTranslator::Produce(FunctionBuilder *builder) {
  DeclareIndexInserter(builder);



  GenIndexInserterFree(builder);
}

void CreateIndexTranslator::Abort(FunctionBuilder *builder) {
  GenIndexInserterFree(builder);
  if (child_translator_ != nullptr) child_translator_->Abort(builder);
  builder->Append(codegen_->ReturnStmt(nullptr));
}

void CreateIndexTranslator::Consume(FunctionBuilder *builder) {
  GenCreateIndex(builder);
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
  auto index_oid = codegen_->Accessor()->CreateIndex(op_->GetNamespaceOid(), op_->GetTableOid(), op_->GetIndexName(), op_->GetSchema());
  // TODO(Wuwen): check if index_oid is valid
  std::vector<ast::Expr *> build_args{codegen_->PointerTo(index_inserter_), index_oid};
  auto index_insert_call = codegen_->BuiltinCall(ast::Builtin::IndexBuild, std::move(build_args));
  auto cond = codegen_->UnaryOp(parsing::Token::Type::BANG, index_insert_call);
  builder->StartIfStmt(cond);
  Abort(builder);
  builder->FinishBlockStmt();
}




}  // namespace terrier::execution::compiler
