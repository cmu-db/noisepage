#pragma once

#include "execution/compiler/compiler_defs.h"
#include "execution/compiler/code_context.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/operator/operator_translator.h"

#include "planner/plannodes/abstract_scan_plan_node.h"

namespace tpl::compiler {

class SeqScanTranslator : public OperatorTranslator{

 public:

  SeqScanTranslator(const terrier::planner::AbstractPlanNode &planNode, Pipeline *pipeline)
  : OperatorTranslator(planNode, pipeline) {}

  void Produce() override;
};


} // namespace tpl::compiler