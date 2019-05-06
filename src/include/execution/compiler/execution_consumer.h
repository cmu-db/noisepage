#pragma once

#include "consumer_context.h"

namespace tpl::compiler {

class CompilationContext;

class ExecutionConsumer {
 public:
  void Prepare(CompilationContext *ctx) {}
  void InitializeQueryState(CompilationContext *ctx) {}
  void TeardownQueryState(CompilationContext *ctx) {}

  void ConsumeResult(ConsumerContext &context, RowBatch &batch) const {}
};

}