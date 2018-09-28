//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_scan_translator.h
//
// Identification: src/include/execution/operator/table_scan_translator.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/compilation_context.h"
#include "execution/consumer_context.h"
#include "execution/operator/operator_translator.h"
#include "execution/scan_callback.h"
#include "execution/table.h"

namespace peloton {

namespace planner {
class SeqScanPlan;
}  // namespace planner

namespace storage {
class DataTable;
}  // namespace storage

namespace codegen {

//===----------------------------------------------------------------------===//
// A translator for sequential table scans
//===----------------------------------------------------------------------===//
class TableScanTranslator : public OperatorTranslator {
 public:
  // Constructor
  TableScanTranslator(const planner::SeqScanPlan &scan,
                      CompilationContext &context, Pipeline &pipeline);

  // Nothing to do here
  void InitializeQueryState() override {}

  // Table scans don't rely on any auxiliary functions
  void DefineAuxiliaryFunctions() override {}

  // The method that produces new tuples
  void Produce() const override;

  // Scans are leaves in the query plan and, hence, do not consume tuples
  void Consume(ConsumerContext &, RowBatch &) const override {}
  void Consume(ConsumerContext &, RowBatch::Row &) const override {}

  // Similar to InitializeQueryState(), table scans don't have any state
  void TearDownQueryState() override {}

 private:
  // Load the table pointer
  llvm::Value *LoadTablePtr(CodeGen &codegen) const;

  // Functions to produce tuples serially or in parallel
  void ProduceSerial() const;
  void ProduceParallel() const;

  // Plan accessor
  const planner::SeqScanPlan &GetScanPlan() const;

 private:
  // Helper class declarations (defined in implementation)
  class AttributeAccess;
  class ScanConsumer;

 private:
  // The code-generating table instance
  codegen::Table table_;
};

}  // namespace codegen
}  // namespace peloton