//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operator_translator.cpp
//
// Identification: src/execution/operator/operator_translator.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/operator/operator_translator.h"

#include "execution/compilation_context.h"

namespace terrier::execution {

OperatorTranslator::OperatorTranslator(const planner::AbstractPlan &plan, CompilationContext &context,
                                       Pipeline &pipeline)
    : plan_(plan), context_(context), pipeline_(pipeline) {
  pipeline.Add(this, Pipeline::Parallelism::Flexible);
}

CodeGen &OperatorTranslator::GetCodeGen() const { return context_.GetCodeGen(); }

llvm::Value *OperatorTranslator::GetExecutorContextPtr() const {
  return context_.GetExecutionConsumer().GetExecutorContextPtr(context_);
}

llvm::Value *OperatorTranslator::GetTransactionPtr() const {
  return context_.GetExecutionConsumer().GetTransactionPtr(context_);
}

llvm::Value *OperatorTranslator::GetStorageManagerPtr() const {
  return context_.GetExecutionConsumer().GetStorageManagerPtr(context_);
}

llvm::Value *OperatorTranslator::GetThreadStatesPtr() const {
  return context_.GetExecutionConsumer().GetThreadStatesPtr(context_);
}

llvm::Value *OperatorTranslator::LoadStatePtr(const QueryState::Id &state_id) const {
  QueryState &query_state = context_.GetQueryState();
  return query_state.LoadStatePtr(GetCodeGen(), state_id);
}

llvm::Value *OperatorTranslator::LoadStateValue(const QueryState::Id &state_id) const {
  QueryState &query_state = context_.GetQueryState();
  return query_state.LoadStateValue(GetCodeGen(), state_id);
}

void OperatorTranslator::Consume(ConsumerContext &context, RowBatch &batch) const {
  batch.Iterate(GetCodeGen(), [this, &context](RowBatch::Row &row) { Consume(context, row); });
}

}  // namespace terrier::execution
