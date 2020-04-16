#include "execution/compiler/operator/cte_scan_translator.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/translator_factory.h"

namespace terrier::execution::compiler {
void CteScanTranslator::Produce(FunctionBuilder *builder) {

  DeclareCteScanIterator(builder);
  child_translator_->Produce(builder);
}

void CteScanTranslator::Consume(FunctionBuilder *builder) {

  // TODO(Gautam, Preetansh, Rohan)
  // Right now we are only generating @CteScanNext(@slot_bottom_query) in the tpl
  // What we need is @slot4 = @CteScanInsert(@slot_bottom_query)
  //builder->Append(codegen_->MakeStmt(codegen_->OneArgCall(ast::Builtin::CteScanNext, child_translator_->GetSlot())));
  parent_translator_->Consume(builder);
}
void CteScanTranslator::DeclareCteScanIterator(FunctionBuilder *builder) {

  // Generate col types
  SetColumnTypes(builder);
  // var cte_scan_iterator : CteScanIterator
  auto cte_scan_iterator_type = codegen_->BuiltinType(ast::BuiltinType::Kind::CteScanIterator);
  builder->Append(codegen_->DeclareVariable(cte_scan_iterator_, cte_scan_iterator_type, nullptr));
  // Call @cteScanIteratorInit
  ast::Expr *cte_scan_iterator_setup = codegen_->CteScanIteratorInit(cte_scan_iterator_, col_types_);
  builder->Append(codegen_->MakeStmt(cte_scan_iterator_setup));
}
void CteScanTranslator::SetColumnTypes(FunctionBuilder *builder) {
  // Declare: var col_types: [num_cols]uint32
  ast::Expr *arr_type = codegen_->ArrayType(all_types_.size(), ast::BuiltinType::Kind::Uint32);
  builder->Append(codegen_->DeclareVariable(col_types_, arr_type, nullptr));

  // For each oid, set col_oids[i] = col_oid
  for (uint16_t i = 0; i < all_types_.size(); i++) {
    ast::Expr *lhs = codegen_->ArrayAccess(col_types_, i);
    ast::Expr *rhs = codegen_->IntLiteral(all_types_[i]);
    builder->Append(codegen_->Assign(lhs, rhs));
  }
}
}  // namespace terrier::execution::compiler
