#include "execution/compiler/operator/index_join_translator.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/translator_factory.h"
#include "planner/plannodes/index_join_plan_node.h"

namespace terrier::execution::compiler {

IndexJoinTranslator::IndexJoinTranslator(const planner::AbstractPlanNode *op, CodeGen *codegen)
    : OperatorTranslator(op, codegen),
      index_join_(static_cast<const terrier::planner::IndexJoinPlanNode *>(op)),
      input_oids_(index_join_->CollectInputOids()),
      table_schema_(codegen_->Accessor()->GetSchema(index_join_->GetTableOid())),
      table_pm_(codegen_->Accessor()->GetTable(index_join_->GetTableOid())->ProjectionMapForOids(input_oids_)),
      index_schema_(codegen_->Accessor()->GetIndexSchema(index_join_->GetIndexOid())),
      index_pm_(codegen_->Accessor()->GetIndex(index_join_->GetIndexOid())->GetKeyOidToOffsetMap()),
      index_iter_(codegen_->NewIdentifier(iter_name_)),
      col_oids_(codegen->NewIdentifier(col_oids_name_)) {}

void IndexJoinTranslator::Produce(FunctionBuilder *builder) {
  // Create the col_oid array
  SetOids(builder);
  // Declare an index iterator
  DeclareIterator(builder);
  // Let child produce
  child_translator_->Produce(builder);
  // Free iterator
  FreeIterator(builder);
}

void IndexJoinTranslator::Consume(FunctionBuilder *builder) {
  // Fill the key with table data
  FillKey(builder);
  // Generate the loop
  GenForLoop(builder);
  bool has_predicate = index_join_->GetJoinPredicate() != nullptr;
  if (has_predicate) GenPredicate(builder);
  // Let parent consume the matching tuples
  parent_translator_->Consume(builder);
  // Close if statement
  if (has_predicate) builder->FinishBlockStmt();
  // Close loop
  builder->FinishBlockStmt();
}

ast::Expr *IndexJoinTranslator::GetOutput(uint32_t attr_idx) {
  auto output_expr = op_->GetOutputSchema()->GetColumn(attr_idx).GetExpr();
  std::unique_ptr<ExpressionTranslator> translator =
      TranslatorFactory::CreateExpressionTranslator(output_expr, codegen_);
  return translator->DeriveExpr(this);
}

ast::Expr *IndexJoinTranslator::GetChildOutput(uint32_t child_idx, uint32_t attr_idx, type::TypeId type) {
  if (child_idx == 0) {
    // For the left child, pass through
    return child_translator_->GetOutput(attr_idx);
  }
  UNREACHABLE("Right child should be accessed using a ColumnValueExpression");
}

ast::Expr *IndexJoinTranslator::GetTableColumn(const catalog::col_oid_t &col_oid) {
  // Call @pciGetType(pci, index)
  auto type = table_schema_.GetColumn(col_oid).Type();
  auto nullable = table_schema_.GetColumn(col_oid).Nullable();
  uint16_t attr_idx = table_pm_[col_oid];
  return codegen_->IndexIteratorGet(index_iter_, type, nullable, attr_idx);
}

void IndexJoinTranslator::SetOids(FunctionBuilder *builder) {
  // Declare: var col_oids: [num_cols]uint32
  ast::Expr *arr_type = codegen_->ArrayType(input_oids_.size(), ast::BuiltinType::Kind::Uint32);
  builder->Append(codegen_->DeclareVariable(col_oids_, arr_type, nullptr));

  // For each oid, set col_oids[i] = col_oid
  for (uint16_t i = 0; i < input_oids_.size(); i++) {
    ast::Expr *lhs = codegen_->ArrayAccess(col_oids_, i);
    ast::Expr *rhs = codegen_->IntLiteral(!input_oids_[i]);
    builder->Append(codegen_->Assign(lhs, rhs));
  }
}

void IndexJoinTranslator::DeclareIterator(FunctionBuilder *builder) {
  // Declare: var index_iter : IndexIterator
  ast::Expr *iter_type = codegen_->BuiltinType(ast::BuiltinType::IndexIterator);
  builder->Append(codegen_->DeclareVariable(index_iter_, iter_type, nullptr));
  // Initialize: @indexIteratorInit(&index_iter, table_oid, index_oid, execCtx)
  auto join_op = dynamic_cast<const planner::IndexJoinPlanNode *>(op_);
  ast::Expr *init_call =
      codegen_->IndexIteratorInit(index_iter_, !join_op->GetTableOid(), !join_op->GetIndexOid(), col_oids_);
  builder->Append(codegen_->MakeStmt(init_call));
}

void IndexJoinTranslator::FillKey(FunctionBuilder *builder) {
  // Set key.attr_i = expr_i for each key attribute
  auto join_op = dynamic_cast<const planner::IndexJoinPlanNode *>(op_);
  for (const auto &key : join_op->GetIndexColumns()) {
    auto translator = TranslatorFactory::CreateExpressionTranslator(key.second.get(), codegen_);
    uint16_t attr_offset = index_pm_.at(key.first);
    type::TypeId attr_type = index_schema_.GetColumn(key.first).Type();
    bool nullable = index_schema_.GetColumn(key.first).Nullable();
    auto set_key_call =
        codegen_->IndexIteratorSetKey(index_iter_, attr_type, nullable, attr_offset, translator->DeriveExpr(this));
    builder->Append(codegen_->MakeStmt(set_key_call));
  }
}

void IndexJoinTranslator::GenForLoop(FunctionBuilder *builder) {
  // for (@indexIteratorScanKey(&index_iter); @indexIteratorAdvance(&index_iter);)
  // Loop Initialization
  ast::Expr *scan_call = codegen_->IndexIteratorScanKey(index_iter_);
  ast::Stmt *loop_init = codegen_->MakeStmt(scan_call);
  // Loop condition
  ast::Expr *has_next_call = codegen_->IndexIteratorAdvance(index_iter_);
  // Make the loop
  builder->StartForStmt(loop_init, has_next_call, nullptr);
}

void IndexJoinTranslator::GenPredicate(FunctionBuilder *builder) {
  auto translator = TranslatorFactory::CreateExpressionTranslator(index_join_->GetJoinPredicate().get(), codegen_);
  ast::Expr *cond = translator->DeriveExpr(this);
  builder->StartIfStmt(cond);
}

void IndexJoinTranslator::FreeIterator(FunctionBuilder *builder) {
  // @indexIteratorFree(&index_iter_)
  ast::Expr *free_call = codegen_->IndexIteratorFree(index_iter_);
  builder->Append(codegen_->MakeStmt(free_call));
}
}  // namespace terrier::execution::compiler