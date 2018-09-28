//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// update_translator.h
//
// Identification: src/include/execution/update_translator.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/compilation_context.h"
#include "execution/operator/operator_translator.h"
#include "execution/pipeline.h"
#include "execution/query_state.h"
#include "execution/table_storage.h"

namespace terrier::execution {

namespace planner {
class UpdatePlan;
}  // namespace planner



class UpdateTranslator : public OperatorTranslator {
 public:
  // Constructor
  UpdateTranslator(const planner::UpdatePlan &plan, CompilationContext &context,
                   Pipeline &pipeline);

  void InitializeQueryState() override;

  // No helper functions
  void DefineAuxiliaryFunctions() override {}

  void Produce() const override;

  void Consume(ConsumerContext &context, RowBatch::Row &row) const override;

  void TearDownQueryState() override;

 private:
  // Target table
  storage::DataTable *target_table_;

  // Runtime state id for the updater
  QueryState::Id updater_state_id_;

  // Tuple storage area
  codegen::TableStorage table_storage_;
};


}  // namespace terrier::execution
