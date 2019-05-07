#include "execution/compiler/operator/seq_scan_translator.h"

namespace tpl::compiler {

  void SeqScanTranslator::InitializeQueryState() {};
  void SeqScanTranslator::TeardownQueryState() {};
  void SeqScanTranslator::Consume(const ConsumerContext *context, RowBatch &batch) const {};


  void SeqScanTranslator::Produce() {
    CodeGen &codegen = pipeline_->GetCodeGen();
    RowBatch rowBatch(codegen);

    auto target = rowBatch.GetIdentifierExpr();
    auto table_name = codegen->NewIdentifierExpr(DUMMY_POS, ast::Identifier("table_1"));
    auto current_fn = codegen.GetCurrentFunction();
    current_fn->StartForInStmt(target, table_name, nullptr);
    ConsumerContext ctx(*pipeline_->GetCompilationContext(), pipeline_);
    ctx.Consume(rowBatch);
  }
} // namespace tpl::compiler