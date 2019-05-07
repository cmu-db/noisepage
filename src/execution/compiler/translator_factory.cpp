#include "execution/compiler/translator_factory.h"

#include "execution/compiler/operator/seq_scan_translator.h"
#include "execution/compiler/pipeline.h"
#include "execution/util/macros.h"

namespace tpl::compiler {

OperatorTranslator *TranslatorFactory::CreateTranslator(const terrier::planner::AbstractPlanNode &op, Pipeline *pipeline) {
    switch (op.GetPlanNodeType()) {
      case terrier::planner::PlanNodeType::SEQSCAN: {
        return new (pipeline->GetRegion()) SeqScanTranslator(op, pipeline);
      }
      default:
        TPL_ASSERT(false, "Unsupported plan node for translation");
    }
  }

ExpressionTranslator *TranslatorFactory::CreateTranslator(const terrier::parser::AbstractExpression &ex) {
  return nullptr;
}

} //namespace tpl::compiler
