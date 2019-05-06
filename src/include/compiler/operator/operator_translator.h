#pragma once

#include "execution/compiler/consumer_context.h"
#include "execution/compiler/pipeline.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "execution/compiler/code_context.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/row_batch.h"

namespace tpl::compiler {

class OperatorTranslator {
 public:

  OperatorTranslator(const terrier::planner::AbstractPlanNode &op, Pipeline *pipeline) : pipeline_(pipeline) {}

  virtual void InitializeQueryState() = 0;
  virtual void TeardownQueryState() = 0;

  virtual void Produce() = 0;

  virtual void Consume(ConsumerContext *context, RowBatch &batch) = 0;

 protected:
  Pipeline *pipeline_;
};

}