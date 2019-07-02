#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/operator/index_join_translator.h"
#include "execution/compiler/translator_factory.h"
#include "execution/compiler/function_builder.h"
#include "planner/plannodes/index_join_plan_node.h"


namespace tpl::compiler {

IndexJoinTranslator::IndexJoinTranslator(const terrier::planner::AbstractPlanNode *op,
                                         tpl::compiler::CodeGen *codegen)
: OperatorTranslator(op, codegen)
, index_iter_(codegen_->NewIdentifier(iter_name_))
, index_struct_(codegen_->NewIdentifier(index_struct_name_))
, index_key_(codegen_->NewIdentifier(index_key_name_)){}

void IndexJoinTranslator::InitializeStructs(tpl::util::RegionVector<tpl::ast::Decl *> *decls) {
  // Create key : type for each index key column
  auto join_op = dynamic_cast<const terrier::planner::IndexJoinPlanNode*>(op_);
  util::RegionVector<ast::FieldDecl *> fields{codegen_->Region()};
  uint32_t attr_idx = 0;
  for (const auto & key: join_op->GetIndexColumns()) {
    ast::Identifier field_name = codegen_->Context()->GetIdentifier(index_key_prefix_ + std::to_string(attr_idx));
    ast::Expr* field_type = codegen_->TplType(key->GetReturnValueType());
    fields.emplace_back(codegen_->MakeField(field_name, field_type));
  }
  decls->emplace_back(codegen_->MakeStruct(index_struct_, std::move(fields)));
}

void IndexJoinTranslator::Produce(tpl::compiler::FunctionBuilder *builder) {
  // First declare an index iterator
  DeclareIterator(builder);
  // Then declare a key
  DeclareKey(builder);
  // Then fill the key with table data
  FillKey(builder);
  // Then generate the loop
  GenForLoop(builder);
  // At the end, free the iterator
  FreeIterator(builder);
}

ast::Expr* IndexJoinTranslator::GetOutput(uint32_t attr_idx) {
  auto output_expr = op_->GetOutputSchema()->GetColumn(attr_idx).GetExpr();
  ExpressionTranslator * translator = TranslatorFactory::CreateExpressionTranslator(output_expr, codegen_);
  return translator->DeriveExpr(this);
}

ast::Expr* IndexJoinTranslator::GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) {
  if (child_idx == 0) {
    // For the left child, pass through
    return prev_translator_->GetOutput(attr_idx);
  } else {
    // For the right child, use @indexIteratorGet call
    return codegen_->IndexIteratorGet(index_iter_, type, attr_idx);
  }
}

void IndexJoinTranslator::DeclareIterator(tpl::compiler::FunctionBuilder *builder) {
  // Declare: var index_iter : IndexIterator
  ast::Expr* iter_type = codegen_->BuiltinType(ast::BuiltinType::IndexIterator);
  builder->Append(codegen_->DeclareVariable(index_iter_, iter_type, nullptr));
  // Initialize: @indexIteratorInit(&index_iter, table_oid, index_oid, execCtx)
  auto join_op = dynamic_cast<const terrier::planner::IndexJoinPlanNode*>(op_);
  ast::Expr* init_call = codegen_->IndexIteratorInit(index_iter_, !join_op->GetTableOid(), !join_op->GetIndexOid());
  builder->Append(codegen_->MakeStmt(init_call));
}

void IndexJoinTranslator::DeclareKey(tpl::compiler::FunctionBuilder *builder) {
  // var index_key : IndexKeyStruct
  builder->Append(codegen_->DeclareVariable(index_key_, codegen_->MakeExpr(index_struct_), nullptr));
}

void IndexJoinTranslator::FillKey(tpl::compiler::FunctionBuilder *builder) {
  // Set key.attr_i = expr_i for each key attribute
  uint32_t attr_idx = 0;
  auto join_op = dynamic_cast<const terrier::planner::IndexJoinPlanNode*>(op_);
  for (const auto & key: join_op->GetIndexColumns()) {
    ast::Identifier attr_identifer = codegen_->Context()->GetIdentifier(index_key_prefix_ + std::to_string(attr_idx));
    ast::Expr * index_key_attr = codegen_->MemberExpr(index_key_, attr_identifer);
    ExpressionTranslator * translator = TranslatorFactory::CreateExpressionTranslator(key.get(), codegen_);
    builder->Append(codegen_->Assign(index_key_attr, translator->DeriveExpr(this)));
  }
}

void IndexJoinTranslator::GenForLoop(tpl::compiler::FunctionBuilder *builder) {
  // for (@indexIteratorScanKey(&index_iter, ...); @indexIteratorAdvance(&index_iter);)
  // Loop Initialization
  // @indexIteratorScanKey(&index_iter, @ptrCast(*int8, &index_key))
  ast::Expr* scan_call = codegen_->IndexIteratorScanKey(index_iter_, index_key_);
  ast::Stmt *loop_init = codegen_->MakeStmt(scan_call);
  // Loop condition
  ast::Expr* has_next_call = codegen_->IndexIteratorAdvance(index_iter_);
  // Make the loop
  builder->StartForStmt(loop_init, has_next_call, nullptr);
}

void IndexJoinTranslator::FreeIterator(tpl::compiler::FunctionBuilder *builder) {
  // @indexIteratorFree(&index_iter_)
  ast::Expr * free_call = codegen_->IndexIteratorFree(index_iter_);
  builder->AppendAfter(codegen_->MakeStmt(free_call));
}
}