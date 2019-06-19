#include "execution/compiler/operator/seq_scan_translator.h"

#include <utility>
#include "execution/compiler/codegen.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/pipeline.h"
#include "execution/ast/type.h"
#include "execution/compiler/translator_factory.h"
#include "planner/plannodes/seq_scan_plan_node.h"

namespace tpl::compiler {

SeqScanTranslator::SeqScanTranslator(const terrier::planner::AbstractPlanNode * op, CodeGen * codegen)
  : OperatorTranslator(op, codegen)
  , tvi_(codegen->NewIdentifier(tvi_name_))
  , pci_(codegen->NewIdentifier(pci_name_))
  , row_(codegen->NewIdentifier(row_name_))
  , table_struct_(codegen->NewIdentifier(table_struct_name_))
  , pci_type_{codegen->Context()->GetIdentifier(pci_type_name_)}
{
}

void SeqScanTranslator::Produce(FunctionBuilder * builder) {
  DeclareTVI(builder);
  GenTVILoop(builder);
  GenTVIClose(builder); // close after the loop
  GenPCILoop(builder);
  GenScanCondition(builder);
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

void SeqScanTranslator::GenPCILoop(FunctionBuilder *builder) {
  // Assign var pci = @tableIterGetPCI(&tvi)
  ast::Expr* get_pci_call = codegen_->TableIterGetPCI(tvi_);
  builder->Append(codegen_->DeclareVariable(pci_, nullptr, get_pci_call));

  // Generate for(; @pciHasNext(pci); @pciAdvance()) {...}
  // The @pciHasNext(pci) call
  ast::Expr* has_next_call = codegen_->PCIHasNext(pci_);
  // The @pciAdvance(pci) call
  ast::Expr* advance_call = codegen_->PCIAdvance(pci_);
  ast::Stmt* loop_advance = codegen_->MakeStmt(advance_call);
  // Make the for loop.
  builder->StartForStmt(nullptr, has_next_call, loop_advance);
}


void SeqScanTranslator::GenScanCondition(FunctionBuilder *builder) {
  // Generate if (cond) {...}
  auto seqscan_op = dynamic_cast<const terrier::planner::SeqScanPlanNode *>(op_);
  auto & predicate = seqscan_op->GetScanPredicate();
  if (predicate == nullptr) return;
  has_predicate_ = true;
  ExpressionTranslator * cond_translator = TranslatorFactory::CreateExpressionTranslator(predicate.get(), codegen_);
  ast::Expr* cond = cond_translator->DeriveExpr(this);
  builder->StartIfStmt(cond);
}


void SeqScanTranslator::GenTVIClose(tpl::compiler::FunctionBuilder *builder) {
  // Generate @tableIterClose(&tvi)
  ast::Expr* close_call = codegen_->TableIterClose(tvi_);
  builder->AppendAfter(codegen_->MakeStmt(close_call));
}

}  // namespace tpl::compiler
