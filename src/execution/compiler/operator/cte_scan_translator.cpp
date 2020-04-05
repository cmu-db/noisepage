#include "execution/compiler/operator/cte_scan_translator.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/translator_factory.h"

namespace terrier::execution::compiler {
void CteScanTranslator::Produce(FunctionBuilder *builder) {
  child_translator_->Produce(builder);
}

void CteScanTranslator::Consume(FunctionBuilder *builder) {
  parent_translator_->Consume(builder);
}
}  // namespace terrier::execution::compiler
