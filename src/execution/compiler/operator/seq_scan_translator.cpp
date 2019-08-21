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

SeqScanTranslator::SeqScanTranslator(const terrier::planner::AbstractPlanNode *op, CodeGen *codegen)
    : OperatorTranslator(op, codegen),
      seqscan_op_(static_cast<const terrier::planner::SeqScanPlanNode *>(op)),
      schema_(codegen->Accessor()->GetSchema(seqscan_op_->GetTableOid())),
      input_oids_(seqscan_op_->CollectInputOids()),
      pm_(codegen->Accessor()->GetTable(seqscan_op_->GetTableOid())->ProjectionMapForOids(input_oids_)),
      has_predicate_(seqscan_op_->GetScanPredicate() != nullptr),
      is_vectorizable_{IsVectorizable(seqscan_op_->GetScanPredicate().get())},
      tvi_(codegen->NewIdentifier(tvi_name_)),
      col_oids_(codegen->NewIdentifier(col_oids_name_)),
      pci_(codegen->NewIdentifier(pci_name_)),
      row_(codegen->NewIdentifier(row_name_)),
      table_struct_(codegen->NewIdentifier(table_struct_name_)),
      pci_type_{codegen->Context()->GetIdentifier(pci_type_name_)} {}

void SeqScanTranslator::Produce(FunctionBuilder *builder) {
  SetOids(builder);
  DeclareTVI(builder);

  // Right now, there is a child translator only for nested loop joins.
  if (child_translator_ != nullptr) {
    // Let it produce
    child_translator_->Produce(builder);
  } else {
    // Otherwise the consume code should be called directly here
    // TODO(Amadou): It's weird to have consume in the sequential scan node.
    Consume(builder);
  }

  // Close iterator
  GenTVIClose(builder);
}

void SeqScanTranslator::Consume(FunctionBuilder *builder) {
  GenTVILoop(builder);
  DeclarePCI(builder);

  // Generate predicate and loop depending on whether we can vectorize or not
  // TODO(Amadou): This logic will more complex if the whole pipeline is vectorized. Move it to a function.
  bool has_if_stmt = false;
  if (is_vectorizable_) {
    if (has_predicate_) GenVectorizedPredicate(builder, seqscan_op_->GetScanPredicate().get());
    GenPCILoop(builder);
  } else {
    GenPCILoop(builder);
    if (has_predicate_) {
      GenScanCondition(builder);
      has_if_stmt = true;
    }
  }
  parent_translator_->Consume(builder);
  // Close predicate if statement
  if (has_if_stmt) {
    builder->FinishBlockStmt();
  }
  // Close PCI loop
  builder->FinishBlockStmt();
  // Close TVI loop
  builder->FinishBlockStmt();
  // May need to reset the iterator if this is a nested loop join
  if (child_translator_ != nullptr) {
    GenTVIReset(builder);
  }
}

ast::Expr *SeqScanTranslator::GetOutput(uint32_t attr_idx) {
  auto output_expr = op_->GetOutputSchema()->GetColumn(attr_idx).GetExpr();
  auto translator = TranslatorFactory::CreateExpressionTranslator(output_expr, codegen_);
  return translator->DeriveExpr(this);
}

ast::Expr *SeqScanTranslator::GetTableColumn(const catalog::col_oid_t &col_oid) {
  // Call @pciGetType(pci, index)
  auto type = schema_.GetColumn(col_oid).Type();
  auto nullable = schema_.GetColumn(col_oid).Nullable();
  uint16_t attr_idx = pm_[col_oid];
  return codegen_->PCIGet(pci_, type, nullable, attr_idx);
}

void SeqScanTranslator::DeclareTVI(FunctionBuilder *builder) {
  // var tvi: TableVectorIterator
  ast::Expr *iter_type = codegen_->BuiltinType(ast::BuiltinType::Kind::TableVectorIterator);
  builder->Append(codegen_->DeclareVariable(tvi_, iter_type, nullptr));

  // Call @tableIterInit(&tvi, execCtx, table_oid, col_oids)
  ast::Expr *init_call = codegen_->TableIterInit(tvi_, !seqscan_op_->GetTableOid(), col_oids_);
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
  ast::Expr *advance_call = codegen_->TableIterAdvance(tvi_);
  // Make the for loop
  builder->StartForStmt(nullptr, advance_call, nullptr);
}

void SeqScanTranslator::DeclarePCI(FunctionBuilder *builder) {
  // Assign var pci = @tableIterGetPCI(&tvi)
  ast::Expr *get_pci_call = codegen_->TableIterGetPCI(tvi_);
  builder->Append(codegen_->DeclareVariable(pci_, nullptr, get_pci_call));
}

void SeqScanTranslator::GenPCILoop(FunctionBuilder *builder) {
  // Generate for(; @pciHasNext(pci); @pciAdvance()) {...} or the Filtered version
  // The @pciHasNext(pci) call
  ast::Expr *has_next_call = codegen_->PCIHasNext(pci_, is_vectorizable_ && has_predicate_);
  // The @pciAdvance(pci) call
  ast::Expr *advance_call = codegen_->PCIAdvance(pci_, is_vectorizable_ && has_predicate_);
  ast::Stmt *loop_advance = codegen_->MakeStmt(advance_call);
  // Make the for loop.
  builder->StartForStmt(nullptr, has_next_call, loop_advance);
}

void SeqScanTranslator::GenScanCondition(FunctionBuilder *builder) {
  auto predicate = seqscan_op_->GetScanPredicate();
  // Regular codegen
  auto cond_translator = TranslatorFactory::CreateExpressionTranslator(predicate.get(), codegen_);
  ast::Expr *cond = cond_translator->DeriveExpr(this);
  builder->StartIfStmt(cond);
}

void SeqScanTranslator::GenTVIClose(execution::compiler::FunctionBuilder *builder) {
  // Generate @tableIterClose(&tvi)
  ast::Expr *close_call = codegen_->TableIterClose(tvi_);
  builder->Append(codegen_->MakeStmt(close_call));
}

void SeqScanTranslator::GenTVIReset(execution::compiler::FunctionBuilder *builder) {
  // Generate @tableIterClose(&tvi)
  ast::Expr *close_call = codegen_->TableIterReset(tvi_);
  builder->Append(codegen_->MakeStmt(close_call));
}

bool SeqScanTranslator::IsVectorizable(const terrier::parser::AbstractExpression *predicate) {
  if (predicate == nullptr) return true;

  if (predicate->GetExpressionType() == terrier::parser::ExpressionType::CONJUNCTION_AND) {
    return IsVectorizable(predicate->GetChild(0).get()) && IsVectorizable(predicate->GetChild(1).get());
  }
  if (COMPARISON_OP(predicate->GetExpressionType())) {
    // left is TVE and right is constant integer.
    // TODO(Amadou): Add support for floats here and in the SIMD code.
    // TODO(Amadou): Support right TVE and left constant integers. Be sure to flip inequalities while codegening.
    return predicate->GetChild(0)->GetExpressionType() == terrier::parser::ExpressionType::COLUMN_VALUE &&
           predicate->GetChild(1)->GetExpressionType() == terrier::parser::ExpressionType::VALUE_CONSTANT &&
           (predicate->GetChild(1)->GetReturnValueType() >= terrier::type::TypeId::TINYINT &&
            predicate->GetChild(1)->GetReturnValueType() <= terrier::type::TypeId::BIGINT);
  }
  return false;
}

void SeqScanTranslator::GenVectorizedPredicate(FunctionBuilder *builder,
                                               const terrier::parser::AbstractExpression *predicate) {
  if (predicate->GetExpressionType() == terrier::parser::ExpressionType::CONJUNCTION_AND) {
    GenVectorizedPredicate(builder, predicate->GetChild(0).get());
    GenVectorizedPredicate(builder, predicate->GetChild(1).get());
  } else if (COMPARISON_OP(predicate->GetExpressionType())) {
    auto left_cve = dynamic_cast<const terrier::parser::ColumnValueExpression *>(predicate->GetChild(0).get());
    auto col_idx = pm_[left_cve->GetColumnOid()];
    auto col_type = schema_.GetColumn(left_cve->GetColumnOid()).Type();
    auto const_val = dynamic_cast<const terrier::parser::ConstantValueExpression *>(predicate->GetChild(1).get());
    auto trans_val = const_val->GetValue();
    auto type = trans_val.Type();
    ast::Expr *filter_val;
    switch (type) {
      case terrier::type::TypeId::TINYINT:
        filter_val = codegen_->IntLiteral(terrier::type::TransientValuePeeker::PeekTinyInt(trans_val));
        break;
      case terrier::type::TypeId::SMALLINT:
        filter_val = codegen_->IntLiteral(terrier::type::TransientValuePeeker::PeekSmallInt(trans_val));
        break;
      case terrier::type::TypeId::INTEGER:
        filter_val = codegen_->IntLiteral(terrier::type::TransientValuePeeker::PeekInteger(trans_val));
        break;
      case terrier::type::TypeId::BIGINT:
        filter_val =
            codegen_->IntLiteral(static_cast<int32_t>(terrier::type::TransientValuePeeker::PeekBigInt(trans_val)));
        break;
      default:
        UNREACHABLE("Impossible vectorized predicate!");
    }
    ast::Expr *filter_call = codegen_->PCIFilter(pci_, predicate->GetExpressionType(), col_idx, col_type, filter_val);
    builder->Append(codegen_->MakeStmt(filter_call));
  }
}
}  // namespace terrier::execution::compiler
