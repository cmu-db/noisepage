#pragma once

#include "execution/compiler/consumer_context.h"
#include "execution/compiler/row_batch.h"

namespace tpl::compiler {
class CompilationContext;
class ConsumerContext;

class ExecutionConsumer {
 public:
  void Prepare(CompilationContext *ctx) {}
  void InitializeQueryState(CompilationContext *ctx) {}
  void TeardownQueryState(CompilationContext *ctx) {}

  void ConsumeResult(ConsumerContext *context, RowBatch *batch) const {}
};

}