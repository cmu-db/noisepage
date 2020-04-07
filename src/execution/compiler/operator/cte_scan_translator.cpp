#include "execution/compiler/operator/cte_scan_translator.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/translator_factory.h"

namespace terrier::execution::compiler {
void CteScanTranslator::Produce(FunctionBuilder *builder) {
  child_translator_->Produce(builder);
}

void CteScanTranslator::Consume(FunctionBuilder *builder) {

  // TODO(Gautam, Preetansh, Rohan)
  // Right now we are only generating @CteScanNext(@slot_bottom_query) in the tpl
  // What we need is @slot4 = @CteScanInsert(@slot_bottom_query)
  //builder->Append(codegen_->MakeStmt(codegen_->OneArgCall(ast::Builtin::CteScanNext, child_translator_->GetSlot())));
  parent_translator_->Consume(builder);
}
}  // namespace terrier::execution::compiler
