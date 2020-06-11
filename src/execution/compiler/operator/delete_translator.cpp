#include "execution/compiler/operator/delete_translator.h"

#include <utility>
#include <vector>

#include "execution/compiler/function_builder.h"
#include "execution/compiler/translator_factory.h"

namespace terrier::execution::compiler {
DeleteTranslator::DeleteTranslator(const terrier::planner::DeletePlanNode *op, CodeGen *codegen)
    : OperatorTranslator(codegen, brain::ExecutionOperatingUnitType::DELETE),
      op_(op),
      deleter_(codegen->NewIdentifier("deleter")),
      col_oids_(codegen->NewIdentifier("col_oids")) {}

void DeleteTranslator::Produce(FunctionBuilder *builder) {
  DeclareDeleter(builder);
  child_translator_->Produce(builder);
  GenDeleterFree(builder);
}

void DeleteTranslator::Abort(FunctionBuilder *builder) {
  GenDeleterFree(builder);
  child_translator_->Abort(builder);
  builder->Append(codegen_->ReturnStmt(nullptr));
}

void DeleteTranslator::Consume(FunctionBuilder *builder) {
  // Delete from table
  GenTableDelete(builder);

  // Delete from every index
  const auto &indexes = codegen_->Accessor()->GetIndexOids(op_->GetTableOid());
  for (auto &index_oid : indexes) {
    GenIndexDelete(builder, index_oid);
  }
}

void DeleteTranslator::DeclareDeleter(terrier::execution::compiler::FunctionBuilder *builder) {
  // Generate col oids
  SetOids(builder);
  // var deleter : StorageInterface
  auto storage_interface_type = codegen_->BuiltinType(ast::BuiltinType::Kind::StorageInterface);
  builder->Append(codegen_->DeclareVariable(deleter_, storage_interface_type, nullptr));
  // Call @storageInterfaceInit
  ast::Expr *deleter_setup = codegen_->StorageInterfaceInit(deleter_, !op_->GetTableOid(), col_oids_, true);
  builder->Append(codegen_->MakeStmt(deleter_setup));
}

void DeleteTranslator::GenDeleterFree(terrier::execution::compiler::FunctionBuilder *builder) {
  // Call @storageInterfaceFree
  ast::Expr *deleter_free = codegen_->OneArgCall(ast::Builtin::StorageInterfaceFree, deleter_, true);
  builder->Append(codegen_->MakeStmt(deleter_free));
}

ast::Expr *DeleteTranslator::GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) {
  TERRIER_ASSERT(child_idx == 0, "Delete plan can only have one child");
  return child_translator_->GetOutput(attr_idx);
}

void DeleteTranslator::GenTableDelete(FunctionBuilder *builder) {
  // Delete from table
  // if (delete not successfull) { Abort(); }
  auto delete_slot = child_translator_->GetSlot();
  std::vector<ast::Expr *> delete_args{codegen_->PointerTo(deleter_), delete_slot};
  auto delete_call = codegen_->BuiltinCall(ast::Builtin::TableDelete, std::move(delete_args));
  auto cond = codegen_->UnaryOp(parsing::Token::Type::BANG, delete_call);
  builder->StartIfStmt(cond);
  Abort(builder);
  builder->FinishBlockStmt();
}

void DeleteTranslator::GenIndexDelete(FunctionBuilder *builder, const catalog::index_oid_t &index_oid) {
  // var delete_index_pr = @getIndexPR(&deleter, oid)
  auto delete_index_pr = codegen_->NewIdentifier("delete_index_pr");
  std::vector<ast::Expr *> pr_call_args{codegen_->PointerTo(deleter_), codegen_->IntLiteral(!index_oid)};
  auto get_index_pr_call = codegen_->BuiltinCall(ast::Builtin::GetIndexPR, std::move(pr_call_args));
  builder->Append(codegen_->DeclareVariable(delete_index_pr, nullptr, get_index_pr_call));

  // Fill up the index pr
  auto index = codegen_->Accessor()->GetIndex(index_oid);
  const auto &index_pm = index->GetKeyOidToOffsetMap();
  const auto &index_schema = codegen_->Accessor()->GetIndexSchema(index_oid);
  const auto &index_cols = index_schema.GetColumns();
  for (const auto &index_col : index_cols) {
    // NOTE: index expressions refer to columns in the child translator.
    // For example, if the child is a seq scan, the index expressions would contain ColumnValueExpressions.
    auto translator = TranslatorFactory::CreateExpressionTranslator(index_col.StoredExpression().Get(), codegen_);
    auto val = translator->DeriveExpr(child_translator_);
    auto pr_set_call = codegen_->PRSet(codegen_->MakeExpr(delete_index_pr), index_col.Type(), index_col.Nullable(),
                                       index_pm.at(index_col.Oid()), val);
    builder->Append(codegen_->MakeStmt(pr_set_call));
  }

  // Delete from index
  std::vector<ast::Expr *> delete_args{codegen_->PointerTo(deleter_), child_translator_->GetSlot()};
  auto index_delete_call = codegen_->BuiltinCall(ast::Builtin::IndexDelete, std::move(delete_args));
  builder->Append(codegen_->MakeStmt(index_delete_call));
}

void DeleteTranslator::SetOids(FunctionBuilder *builder) {
  // Declare: var col_oids: [num_cols]uint32
  ast::Expr *arr_type = codegen_->ArrayType(0, ast::BuiltinType::Kind::Uint32);
  builder->Append(codegen_->DeclareVariable(col_oids_, arr_type, nullptr));
}

}  // namespace terrier::execution::compiler
