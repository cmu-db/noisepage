#include "execution/compiler/operator/seq_scan_translator.h"

#include <utility>

#include "execution/ast/type.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/pipeline.h"
#include "execution/compiler/translator_factory.h"
#include "parser/expression/constant_value_expression.h"
#include "planner/plannodes/seq_scan_plan_node.h"

namespace terrier::execution::compiler {

SeqScanTranslator::SeqScanTranslator(const terrier::planner::SeqScanPlanNode *op, CodeGen *codegen)
    : OperatorTranslator(codegen, brain::ExecutionOperatingUnitType::SEQ_SCAN),
      op_(op),
      schema_(codegen->Accessor()->GetSchema(op_->GetTableOid())),
      input_oids_(MakeInputOids(schema_, op_)),
      pm_(codegen->Accessor()->GetTable(op_->GetTableOid())->ProjectionMapForOids(input_oids_)),
      has_predicate_(op_->GetScanPredicate() != nullptr),
      is_vectorizable_{IsVectorizable(op_->GetScanPredicate().Get())},
      tvi_(codegen->NewIdentifier("tvi")),
      col_oids_(codegen->NewIdentifier("col_oids")),
      vpi_(codegen->NewIdentifier("vpi")),
      slot_(codegen->NewIdentifier("slot")),
      vpi_type_{codegen->Context()->GetIdentifier("VectorProjectionIterator")} {}

void SeqScanTranslator::Produce(FunctionBuilder *builder) {
  SetOids(builder);
  DeclareTVI(builder);

  // There may be a child translator in nested loop joins.
  if (child_translator_ != nullptr) {
    // Let it produce
    child_translator_->Produce(builder);
  } else {
    // Directly do table scan
    DoTableScan(builder);
  }

  // Close iterator
  GenTVIClose(builder);
}

void SeqScanTranslator::Abort(FunctionBuilder *builder) {
  // Close iterator
  GenTVIClose(builder);
  if (child_translator_ != nullptr) child_translator_->Abort(builder);
}

void SeqScanTranslator::DoTableScan(FunctionBuilder *builder) {
  // Start looping over the table
  GenTVILoop(builder);
  DeclareVPI(builder);
  // The VPI loop depends on whether we vectorize or not.
  bool has_if_stmt = false;
  if (is_vectorizable_) {
    if (has_predicate_) GenVectorizedPredicate(builder, op_->GetScanPredicate().Get());
    GenVPILoop(builder);
  } else {
    GenVPILoop(builder);
    if (has_predicate_) {
      GenScanCondition(builder);
      has_if_stmt = true;
    }
  }
  // Declare Slot.
  DeclareSlot(builder);
  // Let parent consume.
  parent_translator_->Consume(builder);
  // Close predicate if statement
  if (has_if_stmt) {
    builder->FinishBlockStmt();
  }
  // Close VPI loop
  builder->FinishBlockStmt();
  // Close TVI loop
  builder->FinishBlockStmt();
}

void SeqScanTranslator::Consume(FunctionBuilder *builder) {
  // This is called in nested loop joins
  DoTableScan(builder);
  // Reset TVI for next iteration.
  GenTVIReset(builder);
}

ast::Expr *SeqScanTranslator::GetOutput(uint32_t attr_idx) {
  auto output_expr = op_->GetOutputSchema()->GetColumn(attr_idx).GetExpr();
  auto translator = TranslatorFactory::CreateExpressionTranslator(output_expr.Get(), codegen_);
  return translator->DeriveExpr(this);
}

ast::Expr *SeqScanTranslator::GetTableColumn(const catalog::col_oid_t &col_oid) {
  // Call @vpiGetType(vpi, index)
  auto type = schema_.GetColumn(col_oid).Type();
  auto nullable = schema_.GetColumn(col_oid).Nullable();
  uint16_t attr_idx = pm_[col_oid];
  return codegen_->VPIGet(vpi_, type, nullable, attr_idx);
}

void SeqScanTranslator::DeclareTVI(FunctionBuilder *builder) {
  // var tvi: TableVectorIterator
  ast::Expr *iter_type = codegen_->BuiltinType(ast::BuiltinType::Kind::TableVectorIterator);
  builder->Append(codegen_->DeclareVariable(tvi_, iter_type, nullptr));

  // Call @tableIterInit(&tvi, execCtx, table_oid, col_oids)
  ast::Expr *init_call = codegen_->TableIterInit(tvi_, !op_->GetTableOid(), col_oids_);
  builder->Append(codegen_->MakeStmt(init_call));
}

void SeqScanTranslator::SetOids(FunctionBuilder *builder) {
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

// Generate for(@tableIterAdvance(&tvi)) {...}
void SeqScanTranslator::GenTVILoop(FunctionBuilder *builder) {
  // The advance call
  ast::Expr *advance_call = codegen_->OneArgCall(ast::Builtin::TableIterAdvance, tvi_, true);
  builder->StartForStmt(nullptr, advance_call, nullptr);
}

void SeqScanTranslator::DeclareVPI(FunctionBuilder *builder) {
  // Assign var vpi = @tableIterGetVPI(&tvi)
  ast::Expr *get_vpi_call = codegen_->OneArgCall(ast::Builtin::TableIterGetVPI, tvi_, true);
  builder->Append(codegen_->DeclareVariable(vpi_, nullptr, get_vpi_call));
}

void SeqScanTranslator::DeclareSlot(FunctionBuilder *builder) {
  // Get var slot = @vpiGetSlot(vpi)
  ast::Expr *get_slot_call = codegen_->OneArgCall(ast::Builtin::VPIGetSlot, vpi_, false);
  builder->Append(codegen_->DeclareVariable(slot_, nullptr, get_slot_call));
}

void SeqScanTranslator::GenVPILoop(FunctionBuilder *builder) {
  // Generate for(; @vpiHasNext(vpi); @vpiAdvance(vpi)) {...} or the Filtered version
  // The HasNext call
  ast::Builtin has_next_fn =
      (is_vectorizable_ && has_predicate_) ? ast::Builtin::VPIHasNextFiltered : ast::Builtin::VPIHasNext;
  ast::Expr *has_next_call = codegen_->OneArgCall(has_next_fn, vpi_, false);
  // The Advance call
  ast::Builtin advance_fn =
      (is_vectorizable_ && has_predicate_) ? ast::Builtin::VPIAdvanceFiltered : ast::Builtin::VPIAdvance;
  ast::Expr *advance_call = codegen_->OneArgCall(advance_fn, vpi_, false);
  ast::Stmt *loop_advance = codegen_->MakeStmt(advance_call);
  // Make the for loop.
  builder->StartForStmt(nullptr, has_next_call, loop_advance);
}

void SeqScanTranslator::GenScanCondition(FunctionBuilder *builder) {
  // Generate tuple at a time scan condition
  auto predicate = op_->GetScanPredicate();
  auto cond_translator = TranslatorFactory::CreateExpressionTranslator(predicate.Get(), codegen_);
  ast::Expr *cond = cond_translator->DeriveExpr(this);
  builder->StartIfStmt(cond);
}

void SeqScanTranslator::GenTVIClose(execution::compiler::FunctionBuilder *builder) {
  // Close iterator
  ast::Expr *close_call = codegen_->OneArgCall(ast::Builtin::TableIterClose, tvi_, true);
  builder->Append(codegen_->MakeStmt(close_call));
}

void SeqScanTranslator::GenTVIReset(execution::compiler::FunctionBuilder *builder) {
  // Reset iterator
  ast::Expr *reset_call = codegen_->OneArgCall(ast::Builtin::TableIterReset, tvi_, true);
  builder->Append(codegen_->MakeStmt(reset_call));
}

bool SeqScanTranslator::IsVectorizable(const terrier::parser::AbstractExpression *predicate) {
  // TODO(Amadou): Does not currently work with negative numbers so it's commented out.
  // Once that bug is fixed, comment back in.
  /*
  // Recursively walks down the query plan to ensure that predicate has the form ((colX comp int) AND (colY comp int)
  ...) if (predicate == nullptr) return true;

  if (predicate->GetExpressionType() == terrier::parser::ExpressionType::CONJUNCTION_AND) {
    return IsVectorizable(predicate->GetChild(0).Get()) && IsVectorizable(predicate->GetChild(1).Get());
  }
  if (TranslatorFactory::IsComparisonOp(predicate->GetExpressionType())) {
    // left is TVE and right is constant integer.
    // TODO(Amadou): Add support for floats here and in the SIMD code.
    // TODO(Amadou): Support right TVE and left constant integers. Be sure to flip inequalities while codegening.
    return predicate->GetChild(0)->GetExpressionType() == terrier::parser::ExpressionType::COLUMN_VALUE &&
        predicate->GetChild(1)->GetExpressionType() == terrier::parser::ExpressionType::VALUE_CONSTANT &&
        (predicate->GetChild(1)->GetReturnValueType() >= terrier::type::TypeId::TINYINT &&
            predicate->GetChild(1)->GetReturnValueType() <= terrier::type::TypeId::BIGINT);
  }*/
  return false;
}

void SeqScanTranslator::GenVectorizedPredicate(FunctionBuilder *builder,
                                               const terrier::parser::AbstractExpression *predicate) {
  if (predicate->GetExpressionType() == terrier::parser::ExpressionType::CONJUNCTION_AND) {
    GenVectorizedPredicate(builder, predicate->GetChild(0).Get());
    GenVectorizedPredicate(builder, predicate->GetChild(1).Get());
  } else if (TranslatorFactory::IsComparisonOp(predicate->GetExpressionType())) {
    auto left_cve = dynamic_cast<const terrier::parser::ColumnValueExpression *>(predicate->GetChild(0).Get());
    auto col_idx = pm_[left_cve->GetColumnOid()];
    auto col_type = schema_.GetColumn(left_cve->GetColumnOid()).Type();
    auto const_val = dynamic_cast<const terrier::parser::ConstantValueExpression *>(predicate->GetChild(1).Get());
    auto type = const_val->GetReturnValueType();
    ast::Expr *filter_val;
    switch (type) {
      case terrier::type::TypeId::TINYINT:
      case terrier::type::TypeId::SMALLINT:
      case terrier::type::TypeId::INTEGER:
      case terrier::type::TypeId::BIGINT:
        filter_val = codegen_->IntLiteral(const_val->Peek<int64_t>());
        break;
      default:
        UNREACHABLE("Impossible vectorized predicate!");
    }
    ast::Expr *filter_call = codegen_->VPIFilter(vpi_, predicate->GetExpressionType(), col_idx, col_type, filter_val);
    builder->Append(codegen_->MakeStmt(filter_call));
  }
  UNREACHABLE("This function should not be called on non vectorized predicates!");
}
}  // namespace terrier::execution::compiler
