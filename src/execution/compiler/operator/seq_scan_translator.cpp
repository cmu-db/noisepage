#include "execution/compiler/operator/seq_scan_translator.h"

#include <utility>
#include "execution/compiler/codegen.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/pipeline.h"
#include "execution/ast/type.h"
#include "execution/compiler/translator_factory.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "parser/expression/constant_value_expression.h"

namespace terrier::execution::compiler {

SeqScanTranslator::SeqScanTranslator(const terrier::planner::AbstractPlanNode * op, CodeGen * codegen)
  : OperatorTranslator(op, codegen)
  , seqscan_op_(static_cast<const terrier::planner::SeqScanPlanNode*>(op))
  , has_predicate_(seqscan_op_->GetScanPredicate() != nullptr)
  , is_vectorizable_{IsVectorizable(seqscan_op_->GetScanPredicate().get())}
  , tvi_(codegen->NewIdentifier(tvi_name_))
  , pci_(codegen->NewIdentifier(pci_name_))
  , row_(codegen->NewIdentifier(row_name_))
  , table_struct_(codegen->NewIdentifier(table_struct_name_))
  , pci_type_{codegen->Context()->GetIdentifier(pci_type_name_)}
{
}

void SeqScanTranslator::Produce(OperatorTranslator * parent, FunctionBuilder * builder) {
  DeclareTVI(builder);
  GenTVILoop(builder);
  GenTVIClose(builder); // close after the loop
  DeclarePCI(builder);

  // Generate predicate and loop depending on whether we can vectorize or not
  bool has_if_stmt = false;
  if (has_predicate_ && is_vectorizable_) {
    GenVectorizedPredicate(builder, seqscan_op_->GetScanPredicate().get());
    GenPCILoop(builder);
  } else if (has_predicate_) {
    GenPCILoop(builder);
    GenScanCondition(builder);
    has_if_stmt = true;
  }
  prev_translator_->Consume(builder);
  builder->FinishBlockStmt();
  if (has_if_stmt) {
    builder->FinishBlockStmt();
  }
}


ast::Expr* SeqScanTranslator::GetOutput(uint32_t attr_idx) {
  auto output_expr = op_->GetOutputSchema()->GetColumn(attr_idx).GetExpr();
  ExpressionTranslator * translator = TranslatorFactory::CreateExpressionTranslator(output_expr, codegen_);
  return translator->DeriveExpr(this);
}

ast::Expr* SeqScanTranslator::GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) {
  TERRIER_ASSERT(child_idx == 0, "SeqScan node only has one child");
  // Call @pciGetType(pci, index)
  return codegen_->PCIGet(pci_, type, attr_idx);
}

void SeqScanTranslator::DeclareTVI(FunctionBuilder * builder) {
  ast::Expr* iter_type = codegen_->BuiltinType(ast::BuiltinType::Kind::TableVectorIterator);
  builder->Append(codegen_->DeclareVariable(tvi_, iter_type, nullptr));
}

// Generate for(@tableIterInit(&tvi, table, execCtx); @tableIterAdvance(&tvi);) {...}
void SeqScanTranslator::GenTVILoop(FunctionBuilder *builder) {
  // The init call
  // TODO: Pass in oids instead of table names
  auto seqscan_op = dynamic_cast<const terrier::planner::SeqScanPlanNode *>(op_);
  ast::Expr* init_call = codegen_->TableIterInit(tvi_, !seqscan_op->GetTableOid());
  ast::Stmt *loop_init = codegen_->MakeStmt(init_call);
  // The advance call
  ast::Expr* advance_call = codegen_->TableIterAdvance(tvi_);
  // Make the for loop
  builder->StartForStmt(loop_init, advance_call, nullptr);
}

void SeqScanTranslator::DeclarePCI(FunctionBuilder *builder) {
  // Assign var pci = @tableIterGetPCI(&tvi)
  ast::Expr* get_pci_call = codegen_->TableIterGetPCI(tvi_);
  builder->Append(codegen_->DeclareVariable(pci_, nullptr, get_pci_call));
}

void SeqScanTranslator::GenPCILoop(FunctionBuilder *builder) {
  // Generate for(; @pciHasNext(pci); @pciAdvance()) {...} or the Filtered version
  // The @pciHasNext(pci) call
  ast::Expr* has_next_call = codegen_->PCIHasNext(pci_, is_vectorizable_ && has_predicate_);
  // The @pciAdvance(pci) call
  ast::Expr* advance_call = codegen_->PCIAdvance(pci_, is_vectorizable_ && has_predicate_);
  ast::Stmt* loop_advance = codegen_->MakeStmt(advance_call);
  // Make the for loop.
  builder->StartForStmt(nullptr, has_next_call, loop_advance);
}


void SeqScanTranslator::GenScanCondition(FunctionBuilder *builder) {
  auto predicate = seqscan_op_->GetScanPredicate();
  // Regular codegen
  ExpressionTranslator * cond_translator = TranslatorFactory::CreateExpressionTranslator(predicate.get(), codegen_);
  ast::Expr* cond = cond_translator->DeriveExpr(this);
  builder->StartIfStmt(cond);
}


void SeqScanTranslator::GenTVIClose(execution::compiler::FunctionBuilder *builder) {
  // Generate @tableIterClose(&tvi)
  ast::Expr* close_call = codegen_->TableIterClose(tvi_);
  builder->AppendAfter(codegen_->MakeStmt(close_call));
}


bool SeqScanTranslator::IsVectorizable(const terrier::parser::AbstractExpression * predicate) {
  if (predicate == nullptr) return true;

  if (predicate->GetExpressionType() == terrier::parser::ExpressionType::CONJUNCTION_AND) {
    return IsVectorizable(predicate->GetChild(0).get()) && IsVectorizable(predicate->GetChild(1).get());
  }
  if (COMPARISON_OP(predicate->GetExpressionType())) {
    // left is TVE and right is constant integer.
    // TODO(Amadou): Add support for floats here and in the SIMD code.
    // TODO(Amadou): Support right TVE and left constant integers. Be sure to flip inequalities while codegening.
    return predicate->GetChild(0)->GetExpressionType() == terrier::parser::ExpressionType::VALUE_TUPLE &&
          predicate->GetChild(1)->GetExpressionType() == terrier::parser::ExpressionType::VALUE_CONSTANT &&
        (predicate->GetChild(1)->GetReturnValueType() >= terrier::type::TypeId::TINYINT &&
         predicate->GetChild(1)->GetReturnValueType() <= terrier::type::TypeId::BIGINT);
  }
  return false;
}

void SeqScanTranslator::GenVectorizedPredicate(FunctionBuilder * builder, const terrier::parser::AbstractExpression * predicate) {
  if (predicate->GetExpressionType() == terrier::parser::ExpressionType::CONJUNCTION_AND) {
    GenVectorizedPredicate(builder, predicate->GetChild(0).get());
    GenVectorizedPredicate(builder, predicate->GetChild(1).get());
  } else if (COMPARISON_OP(predicate->GetExpressionType())) {
    auto left_tve = dynamic_cast<const terrier::parser::ExecTupleValueExpression *>(predicate->GetChild(0).get());
    auto col_idx = left_tve->GetColIdx();
    auto col_type = left_tve->GetReturnValueType();
    auto const_val = dynamic_cast<const terrier::parser::ConstantValueExpression*>(predicate->GetChild(1).get());
    auto trans_val = const_val->GetValue();
    auto type = trans_val.Type();
    ast::Expr * filter_val;
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
        filter_val = codegen_->IntLiteral(static_cast<int32_t>(terrier::type::TransientValuePeeker::PeekBigInt(trans_val)));
        break;
      default:
        UNREACHABLE("Impossible vectorized predicate!");
    }
    ast::Expr * filter_call = codegen_->PCIFilter(pci_, predicate->GetExpressionType(), col_idx, col_type, filter_val);
    builder->Append(codegen_->MakeStmt(filter_call));
  }
}
}  // namespace terrier::execution::compiler
