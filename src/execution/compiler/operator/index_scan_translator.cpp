#include "execution/compiler/operator/index_scan_translator.h"

#include <memory>
#include <unordered_map>

#include "catalog/catalog_accessor.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/translator_factory.h"
#include "planner/plannodes/index_join_plan_node.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"

namespace terrier::execution::compiler {

IndexScanTranslator::IndexScanTranslator(const planner::IndexScanPlanNode *op, CodeGen *codegen)
    : OperatorTranslator(codegen, brain::ExecutionOperatingUnitType::IDX_SCAN),
      op_(op),
      input_oids_(op_->GetColumnOids()),
      table_schema_(codegen_->Accessor()->GetSchema(op_->GetTableOid())),
      table_pm_(codegen_->Accessor()->GetTable(op_->GetTableOid())->ProjectionMapForOids(input_oids_)),
      index_schema_(codegen_->Accessor()->GetIndexSchema(op_->GetIndexOid())),
      index_pm_(codegen_->Accessor()->GetIndex(op_->GetIndexOid())->GetKeyOidToOffsetMap()),
      index_iter_(codegen_->NewIdentifier("index_iter")),
      col_oids_(codegen->NewIdentifier("col_oids")),
      index_pr_(codegen->NewIdentifier("index_pr")),
      lo_index_pr_(codegen->NewIdentifier("lo_index_pr")),
      hi_index_pr_(codegen->NewIdentifier("hi_index_pr")),
      table_pr_(codegen->NewIdentifier("table_pr")),
      pr_type_(codegen->Context()->GetIdentifier("ProjectedRow")),
      slot_(codegen->NewIdentifier("slot")) {}

void IndexScanTranslator::Produce(FunctionBuilder *builder) {
  // Create the col_oid array
  SetOids(builder);
  // Declare an index iterator
  DeclareIterator(builder);
  // Get the index prs
  DeclareIndexPR(builder);
  // Let child produce
  if (child_translator_ != nullptr) {
    child_translator_->Produce(builder);
  } else {
    Consume(builder);
  }
  // Free iterator
  FreeIterator(builder);
}

void IndexScanTranslator::Abort(FunctionBuilder *builder) {
  // Free iterator
  FreeIterator(builder);
  if (child_translator_ != nullptr) child_translator_->Abort(builder);
}

void IndexScanTranslator::Consume(FunctionBuilder *builder) {
  // Fill the key with table data
  if (op_->GetScanType() == planner::IndexScanType::Exact) {
    FillKey(builder, index_pr_, op_->GetIndexColumns());
  } else {
    FillKey(builder, lo_index_pr_, op_->GetLoIndexColumns());
    FillKey(builder, hi_index_pr_, op_->GetHiIndexColumns());
  }
  // Generate the loop
  GenForLoop(builder);
  // Get Table PR
  DeclareTablePR(builder);
  DeclareSlot(builder);
  bool has_predicate = op_->GetScanPredicate() != nullptr;
  if (has_predicate) GenPredicate(builder);
  // Let parent consume the matching tuples
  parent_translator_->Consume(builder);
  // Close if statement
  if (has_predicate) builder->FinishBlockStmt();
  // Close loop
  builder->FinishBlockStmt();
}

ast::Expr *IndexScanTranslator::GetOutput(uint32_t attr_idx) {
  auto output_expr = op_->GetOutputSchema()->GetColumn(attr_idx).GetExpr();
  std::unique_ptr<ExpressionTranslator> translator =
      TranslatorFactory::CreateExpressionTranslator(output_expr.Get(), codegen_);
  return translator->DeriveExpr(this);
}

ast::Expr *IndexScanTranslator::GetChildOutput(uint32_t child_idx, uint32_t attr_idx, type::TypeId type) {
  UNREACHABLE("IndexScan nodes should use column value expressions");
}

ast::Expr *IndexScanTranslator::GetTableColumn(const catalog::col_oid_t &col_oid) {
  auto type = table_schema_.GetColumn(col_oid).Type();
  auto nullable = table_schema_.GetColumn(col_oid).Nullable();
  uint16_t attr_idx = table_pm_[col_oid];
  return codegen_->PRGet(codegen_->MakeExpr(table_pr_), type, nullable, attr_idx);
}

void IndexScanTranslator::SetOids(FunctionBuilder *builder) {
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

void IndexScanTranslator::DeclareIterator(FunctionBuilder *builder) {
  // Declare: var index_iter : IndexIterator
  ast::Expr *iter_type = codegen_->BuiltinType(ast::BuiltinType::IndexIterator);
  builder->Append(codegen_->DeclareVariable(index_iter_, iter_type, nullptr));
  // Initialize: @indexIteratorInit(&index_iter, table_oid, index_oid, execCtx, col_oids_)
  uint32_t num_attrs = 0;
  if (op_->GetScanType() == planner::IndexScanType::Exact) {
    num_attrs = op_->GetIndexColumns().size();
  } else {
    num_attrs = std::max(op_->GetLoIndexColumns().size(), op_->GetHiIndexColumns().size());
  }

  ast::Expr *init_call =
      codegen_->IndexIteratorInit(index_iter_, num_attrs, !op_->GetTableOid(), !op_->GetIndexOid(), col_oids_);
  builder->Append(codegen_->MakeStmt(init_call));
}

void IndexScanTranslator::DeclareIndexPR(terrier::execution::compiler::FunctionBuilder *builder) {
  if (op_->GetScanType() == planner::IndexScanType::Exact) {
    ast::Expr *get_pr_call = codegen_->OneArgCall(ast::Builtin::IndexIteratorGetPR, index_iter_, true);
    builder->Append(codegen_->DeclareVariable(index_pr_, nullptr, get_pr_call));
  } else {
    ast::Expr *lo_pr_call = codegen_->OneArgCall(ast::Builtin::IndexIteratorGetLoPR, index_iter_, true);
    ast::Expr *hi_pr_call = codegen_->OneArgCall(ast::Builtin::IndexIteratorGetHiPR, index_iter_, true);
    builder->Append(codegen_->DeclareVariable(lo_index_pr_, nullptr, lo_pr_call));
    builder->Append(codegen_->DeclareVariable(hi_index_pr_, nullptr, hi_pr_call));
  }
}

void IndexScanTranslator::DeclareTablePR(terrier::execution::compiler::FunctionBuilder *builder) {
  ast::Expr *get_pr_call = codegen_->OneArgCall(ast::Builtin::IndexIteratorGetTablePR, index_iter_, true);
  builder->Append(codegen_->DeclareVariable(table_pr_, nullptr, get_pr_call));
}

void IndexScanTranslator::DeclareSlot(terrier::execution::compiler::FunctionBuilder *builder) {
  ast::Expr *get_slot_call = codegen_->OneArgCall(ast::Builtin::IndexIteratorGetSlot, index_iter_, true);
  builder->Append(codegen_->DeclareVariable(slot_, nullptr, get_slot_call));
}

void IndexScanTranslator::FillKey(
    FunctionBuilder *builder, ast::Identifier pr,
    const std::unordered_map<catalog::indexkeycol_oid_t, planner::IndexExpression> &index_exprs) {
  // Set key.attr_i = expr_i for each key attribute
  for (const auto &key : index_exprs) {
    auto translator = TranslatorFactory::CreateExpressionTranslator(key.second.Get(), codegen_);
    uint16_t attr_offset = index_pm_.at(key.first);
    type::TypeId attr_type = index_schema_.GetColumn(!key.first - 1).Type();
    bool nullable = index_schema_.GetColumn(!key.first - 1).Nullable();
    auto set_key_call =
        codegen_->PRSet(codegen_->MakeExpr(pr), attr_type, nullable, attr_offset, translator->DeriveExpr(this));
    builder->Append(codegen_->MakeStmt(set_key_call));
  }
}

void IndexScanTranslator::GenForLoop(FunctionBuilder *builder) {
  // for (@indexIteratorScanKey(&index_iter); @indexIteratorAdvance(&index_iter);)
  // Loop Initialization
  ast::Expr *scan_call = codegen_->IndexIteratorScan(index_iter_, op_->GetScanType(), op_->ScanLimit());

  ast::Stmt *loop_init = codegen_->MakeStmt(scan_call);
  // Loop condition
  ast::Expr *advance_call = codegen_->OneArgCall(ast::Builtin::IndexIteratorAdvance, index_iter_, true);
  // Make the loop
  builder->StartForStmt(loop_init, advance_call, nullptr);
}

void IndexScanTranslator::GenPredicate(FunctionBuilder *builder) {
  auto translator = TranslatorFactory::CreateExpressionTranslator(op_->GetScanPredicate().Get(), codegen_);
  ast::Expr *cond = translator->DeriveExpr(this);
  builder->StartIfStmt(cond);
}

void IndexScanTranslator::FreeIterator(FunctionBuilder *builder) {
  // @indexIteratorFree(&index_iter_)
  ast::Expr *free_call = codegen_->OneArgCall(ast::Builtin::IndexIteratorFree, index_iter_, true);
  builder->Append(codegen_->MakeStmt(free_call));
}
}  // namespace terrier::execution::compiler
