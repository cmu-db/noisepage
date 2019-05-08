#pragma once

#include "execution/compiler/operator/operator_translator.h"

namespace tpl::compiler {

class SeqScanTranslator : public OperatorTranslator {

 public:
  SeqScanTranslator(const terrier::planner::AbstractPlanNode &op, Pipeline *pipeline)
  : OperatorTranslator(op, pipeline) {}

  void InitializeQueryState() override {}
  void TeardownQueryState() override {}
  void Consume(const ConsumerContext *context, RowBatch &batch) const override {}

  void Produce() override;
};


} // namespace tpl::compiler