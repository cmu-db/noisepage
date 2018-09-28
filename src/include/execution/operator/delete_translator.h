//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delete_translator.h
//
// Identification: src/include/execution/operator/delete_translator.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/operator/operator_translator.h"
#include "execution/table.h"

namespace terrier::execution {

namespace planner {
class DeletePlan;
}  // namespace planner

//===----------------------------------------------------------------------===//
// The translator for a delete operator
//===----------------------------------------------------------------------===//
class DeleteTranslator : public OperatorTranslator {
 public:
  DeleteTranslator(const planner::DeletePlan &delete_plan, CompilationContext &context, Pipeline &pipeline);

  void InitializeQueryState() override;

  void DefineAuxiliaryFunctions() override {}

  void TearDownQueryState() override {}

  void Produce() const override;

  void Consume(ConsumerContext &, RowBatch::Row &) const override;

 private:
  // Table accessor
  Table table_;

  // The Deleter instance
  QueryState::Id deleter_state_id_;
};

}  // namespace terrier::execution
