#pragma once
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>
#include "brain/brain_defs.h"
#include "execution/exec_defs.h"

namespace terrier::execution::compiler::test {
class CompilerTest_SimpleSeqScanTest_Test;
class CompilerTest_SimpleSeqScanWithProjectionTest_Test;
class CompilerTest_SimpleSeqScanWithParamsTest_Test;
class CompilerTest_SimpleIndexScanTest_Test;
class CompilerTest_SimpleIndexScanAsendingTest_Test;
class CompilerTest_SimpleIndexScanLimitAsendingTest_Test;
class CompilerTest_SimpleIndexScanDesendingTest_Test;
class CompilerTest_SimpleIndexScanLimitDesendingTest_Test;
class CompilerTest_SimpleAggregateTest_Test;
class CompilerTest_CountStarTest_Test;
class CompilerTest_SimpleSortTest_Test;
class CompilerTest_SimpleAggregateHavingTest_Test;
class CompilerTest_SimpleHashJoinTest_Test;
class CompilerTest_MultiWayHashJoinTest_Test;
class CompilerTest_SimpleNestedLoopJoinTest_Test;
class CompilerTest_SimpleIndexNestedLoopJoinTest_Test;
class CompilerTest_SimpleIndexNestedLoopJoinMultiColumnTest_Test;
class CompilerTest_SimpleDeleteTest_Test;
class CompilerTest_SimpleUpdateTest_Test;
class CompilerTest_SimpleInsertTest_Test;
class CompilerTest_InsertIntoSelectWithParamTest_Test;
class CompilerTest_SimpleInsertWithParamsTest_Test;
class CompilerTest_StaticDistinctAggregateTest_Test;
}  // namespace terrier::execution::compiler::test

namespace terrier::optimizer {
class IdxJoinTest_SimpleIdxJoinTest_Test;
}

namespace terrier::brain {

class OperatingUnitRecorder;

/**
 * ExecutionOperatingUnitFeature is used to record a single operating unit.
 * i.e a ExecutionOperatingUnitFeature captures a single high-level operation
 * performed during an execution of a given pipeline.
 *
 * An ExecutionOperatingUnitFeature captures the following three metadata
 * about any given operating unit in a pipeline:
 * - Type
 * - Estimated number of tuples
 * - Total Key Size
 * - Number of keys
 * - Estimated cardinality
 */
class ExecutionOperatingUnitFeature {
  friend class PipelineOperatingUnits;
  friend class OperatingUnitRecorder;

 public:
  /**
   * Constructor for ExecutionOperatingUnitFeature
   * @param feature Type
   * @param num_rows Estimated number of output tuples
   * @param key_size Total Key Size
   * @param num_keys Number of keys
   * @param cardinality Estimated cardinality
   * @param mem_factor Memory adjustment factor
   */
  ExecutionOperatingUnitFeature(ExecutionOperatingUnitType feature, size_t num_rows, size_t key_size, size_t num_keys,
                                size_t cardinality, double mem_factor)
      : feature_(feature),
        num_rows_(num_rows),
        key_size_(key_size),
        num_keys_(num_keys),
        cardinality_(cardinality),
        mem_factors_({mem_factor}) {}

  /**
   * @returns type
   */
  ExecutionOperatingUnitType GetExecutionOperatingUnitType() const { return feature_; }

  /**
   * @returns estimated number of output tuples
   */
  size_t GetNumRows() const { return num_rows_; }

  /**
   * @return total key size
   */
  size_t GetKeySize() const { return key_size_; }

  /**
   * @return number of keys (columns)
   */
  size_t GetNumKeys() const { return num_keys_; }

  /**
   * @returns estimated cardinality
   */
  size_t GetCardinality() const { return cardinality_; }

  /**
   * @returns memory adjustment factor
   */
  double GetMemFactor() const {
    if (mem_factors_.empty()) return 1.0;

    double sum = 0.0;
    for (auto factor : mem_factors_) {
      sum += factor;
    }

    return sum / mem_factors_.size();
  }

 private:
  /**
   * Set the estimated number of output tuples
   * @note only should be invoked by OperatingUnitRecorder
   * @param num_rows Updated estimate
   */
  void SetNumRows(size_t num_rows) { num_rows_ = num_rows; }

  /**
   * Set the estimated cardinality
   * @note only should be invoked by OperatingUnitRecorder
   * @param cardinality Updated cardinality
   */
  void SetCardinality(size_t cardinality) { cardinality_ = cardinality; }

  /**
   * Set the mem factor
   * @note only should be invoked by OperatingUnitRecorder
   * @param mem_factor Updated mem_factor
   */
  void AddMemFactor(double mem_factor) { mem_factors_.emplace_back(mem_factor); }

  ExecutionOperatingUnitType feature_;
  size_t num_rows_;
  size_t key_size_;
  size_t num_keys_;
  size_t cardinality_;
  std::vector<double> mem_factors_;
};

/**
 * Convenience typedef for a vector of features
 */
using ExecutionOperatingUnitFeatureVector = std::vector<ExecutionOperatingUnitFeature>;

/**
 * PipelineOperatingUnits manages the storage/association of specific pipeline
 * identifiers to the list of ExecutionOperatingUnitFeatures that are contained
 * within that given pipeline.
 */
class PipelineOperatingUnits {
 public:
  friend class terrier::optimizer::IdxJoinTest_SimpleIdxJoinTest_Test;
  friend class terrier::execution::compiler::test::CompilerTest_SimpleSeqScanTest_Test;
  friend class terrier::execution::compiler::test::CompilerTest_SimpleSeqScanWithProjectionTest_Test;
  friend class terrier::execution::compiler::test::CompilerTest_SimpleSeqScanWithParamsTest_Test;
  friend class terrier::execution::compiler::test::CompilerTest_SimpleIndexScanTest_Test;
  friend class terrier::execution::compiler::test::CompilerTest_SimpleIndexScanAsendingTest_Test;
  friend class terrier::execution::compiler::test::CompilerTest_SimpleIndexScanLimitAsendingTest_Test;
  friend class terrier::execution::compiler::test::CompilerTest_SimpleIndexScanDesendingTest_Test;
  friend class terrier::execution::compiler::test::CompilerTest_SimpleIndexScanLimitDesendingTest_Test;
  friend class terrier::execution::compiler::test::CompilerTest_SimpleAggregateTest_Test;
  friend class terrier::execution::compiler::test::CompilerTest_CountStarTest_Test;
  friend class terrier::execution::compiler::test::CompilerTest_SimpleSortTest_Test;
  friend class terrier::execution::compiler::test::CompilerTest_SimpleAggregateHavingTest_Test;
  friend class terrier::execution::compiler::test::CompilerTest_SimpleHashJoinTest_Test;
  friend class terrier::execution::compiler::test::CompilerTest_MultiWayHashJoinTest_Test;
  friend class terrier::execution::compiler::test::CompilerTest_SimpleNestedLoopJoinTest_Test;
  friend class terrier::execution::compiler::test::CompilerTest_SimpleIndexNestedLoopJoinTest_Test;
  friend class terrier::execution::compiler::test::CompilerTest_SimpleIndexNestedLoopJoinMultiColumnTest_Test;
  friend class terrier::execution::compiler::test::CompilerTest_SimpleDeleteTest_Test;
  friend class terrier::execution::compiler::test::CompilerTest_SimpleUpdateTest_Test;
  friend class terrier::execution::compiler::test::CompilerTest_SimpleInsertTest_Test;
  friend class terrier::execution::compiler::test::CompilerTest_InsertIntoSelectWithParamTest_Test;
  friend class terrier::execution::compiler::test::CompilerTest_SimpleInsertWithParamsTest_Test;
  friend class terrier::execution::compiler::test::CompilerTest_StaticDistinctAggregateTest_Test;

  /**
   * Constructor
   */
  PipelineOperatingUnits() = default;

  /**
   * Adds a new pipeline and its features vector to the storage unit
   * @note asserts that pipeline identifier is unique
   * @param pipeline pipeline identifier
   * @param features Vector of ExecutionOperatingUnitFeature describing pipeline contents
   */
  void RecordOperatingUnit(execution::pipeline_id_t pipeline, ExecutionOperatingUnitFeatureVector &&features) {
    UNUSED_ATTRIBUTE auto res = units_.insert(std::make_pair(pipeline, std::move(features)));
    TERRIER_ASSERT(res.second, "Recording duplicate pipeline entry into PipelineOperatingUnits");
  }

  /**
   * Gets the features vector for a given pipeline identifier
   * @note asserts that pipeline identifier exists
   * @param pipeline pipeline identifier
   */
  const ExecutionOperatingUnitFeatureVector &GetPipelineFeatures(execution::pipeline_id_t pipeline) const {
    UNUSED_ATTRIBUTE auto itr = units_.find(pipeline);
    TERRIER_ASSERT(itr != units_.end(), "Requested pipeline could not be found in PipelineOperatingUnits");
    return itr->second;
  }

 private:
  std::unordered_map<execution::pipeline_id_t, ExecutionOperatingUnitFeatureVector> units_{};
};

}  // namespace terrier::brain
