#include "execution/compiler/expression/seq_scanner.h"

namespace terrier::execution::compiler {
void SeqScanner::DeclareTVI(FunctionBuilder *builder, uint32_t table_oid, ast::Identifier col_oid) {
  // var tvi: TableVectorIterator
  ast::Expr *iter_type = codegen_->BuiltinType(ast::BuiltinType::Kind::TableVectorIterator);
  builder->Append(codegen_->DeclareVariable(tvi_, iter_type, nullptr));

  // Call @tableIterInit(&tvi, execCtx, table_oid, col_oids)
  ast::Expr *init_call = codegen_->TableIterInit(tvi_, table_oid, col_oid);
  builder->Append(codegen_->MakeStmt(init_call));
}

void SeqScanner::TVILoop(FunctionBuilder *builder) {
  // The advance call
  ast::Expr *advance_call = codegen_->OneArgCall(ast::Builtin::TableIterAdvance, tvi_, true);
  builder->StartForStmt(nullptr, advance_call, nullptr);
}

void SeqScanner::DeclarePCI(FunctionBuilder *builder) {
  // Assign var pci = @tableIterGetPCI(&tvi)
  ast::Expr *get_pci_call = codegen_->OneArgCall(ast::Builtin::TableIterGetPCI, tvi_, true);
  builder->Append(codegen_->DeclareVariable(pci_, nullptr, get_pci_call));
}

void SeqScanner::PCILoop(FunctionBuilder *builder) {
  // Generate for(; @pciHasNext(pci); @pciAdvance(pci)) {...} or the Filtered version
  // The HasNext call
  ast::Expr *has_next_call = codegen_->OneArgCall(ast::Builtin::PCIHasNext, pci_, false);
  // The Advance call
  ast::Expr *advance_call = codegen_->OneArgCall(ast::Builtin::PCIAdvance, pci_, false);
  ast::Stmt *loop_advance = codegen_->MakeStmt(advance_call);
  // Make the for loop.
  builder->StartForStmt(nullptr, has_next_call, loop_advance);
}

void SeqScanner::PCILoopCondition(FunctionBuilder *builder, bool is_vectorizable, bool has_predicate) {
  // Generate for(; @pciHasNext(pci); @pciAdvance(pci)) {...} or the Filtered version
  // The HasNext call
  ast::Builtin has_next_fn =
      (is_vectorizable && has_predicate) ? ast::Builtin::PCIHasNextFiltered : ast::Builtin::PCIHasNext;
  ast::Expr *has_next_call = codegen_->OneArgCall(has_next_fn, pci_, false);
  // The Advance call
  ast::Builtin advance_fn =
      (is_vectorizable && has_predicate) ? ast::Builtin::PCIAdvanceFiltered : ast::Builtin::PCIAdvance;
  ast::Expr *advance_call = codegen_->OneArgCall(advance_fn, pci_, false);
  ast::Stmt *loop_advance = codegen_->MakeStmt(advance_call);
  // Make the for loop.
  builder->StartForStmt(nullptr, has_next_call, loop_advance);
}

ast::Identifier SeqScanner::DeclareSlot(FunctionBuilder *builder) {
  // Get var slot = @pciGetSlot(pci)
  ast::Expr *get_slot_call = codegen_->OneArgCall(ast::Builtin::PCIGetSlot, pci_, false);
  builder->Append(codegen_->DeclareVariable(slot_, nullptr, get_slot_call));
  return slot_;
}

void SeqScanner::TVIClose(FunctionBuilder *builder) {
  // Close iterator
  ast::Expr *close_call = codegen_->OneArgCall(ast::Builtin::TableIterClose, tvi_, true);
  builder->Append(codegen_->MakeStmt(close_call));
}

void SeqScanner::TVIReset(FunctionBuilder *builder) {
  ast::Expr *reset_call = codegen_->OneArgCall(ast::Builtin::TableIterReset, tvi_, true);
  builder->Append(codegen_->MakeStmt(reset_call));
}

}  // namespace terrier::execution::compiler
