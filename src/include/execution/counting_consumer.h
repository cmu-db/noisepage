//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// counting_consumer.h
//
// Identification: src/include/execution/counting_consumer.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/execution_consumer.h"

namespace terrier::execution {

//===----------------------------------------------------------------------===//
// A consumer that just counts the number of results
//===----------------------------------------------------------------------===//
class CountingConsumer : public ExecutionConsumer {
 public:
  bool SupportsParallelExec() const override { return false; }

  void Prepare(CompilationContext &compilation_context) override;

  void InitializeQueryState(CompilationContext &context) override;

  void TearDownQueryState(CompilationContext &) override {}

  void ConsumeResult(ConsumerContext &context, RowBatch::Row &row) const override;

  uint64_t GetCount() const { return counter_; }
  void ResetCount() { counter_ = 0; }
  char *GetConsumerState() final { return reinterpret_cast<char *>(&counter_); }

 private:
  llvm::Value *GetCounterState(CodeGen &codegen, QueryState &query_state) const;

 private:
  uint64_t counter_;
  // The slot in the runtime state to find our state context
  QueryState::Id counter_state_id_;
};

}  // namespace terrier::execution
