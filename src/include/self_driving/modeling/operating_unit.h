#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/exec_defs.h"
#include "execution/sql/memory_pool.h"
#include "execution/util/execution_common.h"
#include "self_driving/modeling/operating_unit_defs.h"

namespace noisepage::runner {
class ExecutionRunners;
}  // namespace noisepage::runner

namespace noisepage::execution::compiler::test {
class CompilerTest_SimpleSeqScanTest_Test;
class CompilerTest_SimpleSeqScanNonVecFilterTest_Test;
class CompilerTest_SimpleSeqScanWithProjectionTest_Test;
class CompilerTest_SimpleSeqScanWithParamsTest_Test;
class CompilerTest_SimpleIndexScanTest_Test;
class CompilerTest_SimpleIndexScanAscendingTest_Test;
class CompilerTest_SimpleIndexScanLimitAscendingTest_Test;
class CompilerTest_SimpleIndexScanDescendingTest_Test;
class CompilerTest_SimpleIndexScanLimitDescendingTest_Test;
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
}  // namespace noisepage::execution::compiler::test

namespace noisepage::execution::exec {
class ExecutionContext;
}  // namespace noisepage::execution::exec

namespace noisepage::execution::sql {
class TableVectorIterator;
class Sorter;
class JoinHashTable;
class AggregationHashTable;
}  // namespace noisepage::execution::sql

namespace noisepage::optimizer {
class IdxJoinTest_SimpleIdxJoinTest_Test;
}  // namespace noisepage::optimizer

namespace noisepage::selfdriving {

class ExecOUFeatureVector;
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
  friend class noisepage::runner::ExecutionRunners;
  friend class execution::exec::ExecutionContext;
  friend class OperatingUnitRecorder;
  friend class ExecOUFeatureVector;
  friend class PipelineOperatingUnits;
  friend class execution::sql::TableVectorIterator;
  friend class execution::sql::Sorter;
  friend class execution::sql::JoinHashTable;
  friend class execution::sql::AggregationHashTable;

 public:
  ExecutionOperatingUnitFeature() = default;

  /**
   * Constructor for ExecutionOperatingUnitFeature
   * @param translator_id The ID of the translator
   * @param feature Type
   * @param num_rows Estimated number of output tuples
   * @param key_size Total Key Size
   * @param num_keys Number of keys
   * @param cardinality Estimated cardinality
   * @param mem_factor Memory adjustment factor
   * @param num_loops Number of loops
   * @param num_concurrent Number of concurrent tasks (including current one)
   */
  ExecutionOperatingUnitFeature(execution::translator_id_t translator_id, ExecutionOperatingUnitType feature,
                                size_t num_rows, size_t key_size, size_t num_keys, size_t cardinality,
                                double mem_factor, size_t num_loops, size_t num_concurrent)
      : translator_id_(translator_id),
        feature_id_(feature_id_counter++),
        feature_(feature),
        num_rows_(num_rows),
        key_size_(key_size),
        num_keys_(num_keys),
        cardinality_(cardinality),
        mem_factors_({mem_factor}),
        num_loops_(num_loops),
        num_concurrent_(num_concurrent) {}

  /**
   * Constructor for ExecutionOperatingUnitFeature from an existing feature
   * @note Does not copy num_rows, cardinality
   *
   * @param feature Newly created OU type
   * @param other Existing OU to copy information from
   */
  ExecutionOperatingUnitFeature(ExecutionOperatingUnitType feature, const ExecutionOperatingUnitFeature &other)
      : translator_id_(other.translator_id_),
        feature_id_(other.feature_id_),
        feature_(feature),
        num_rows_(0),
        key_size_(other.key_size_),
        num_keys_(other.num_keys_),
        cardinality_(0),
        mem_factors_(other.mem_factors_),
        num_loops_(other.num_loops_),
        num_concurrent_(other.num_concurrent_) {}

  /**
   * Returns a vector of doubles consisting of 7 features starting with num_rows
   */
  void GetAllAttributes(std::vector<double> *all_attributes) const {
    all_attributes->push_back(num_rows_);
    all_attributes->push_back(key_size_);
    all_attributes->push_back(num_keys_);
    all_attributes->push_back(cardinality_);
    all_attributes->push_back(GetMemFactor());
    all_attributes->push_back(num_loops_);
    all_attributes->push_back(num_concurrent_);
  }

  /** @return The ID of the translator for this ExecutionOperatingUnitFeature. */
  execution::translator_id_t GetTranslatorId() const { return translator_id_; }

  /** @return The ID of this ExecutionOperatingUnitFeature. */
  execution::feature_id_t GetFeatureId() const { return feature_id_; }

  /**
   * @return type
   */
  ExecutionOperatingUnitType GetExecutionOperatingUnitType() const { return feature_; }

  /**
   * Updates target with the value of update under a specific update mode.
   *
   * If mode == SET: *target = update
   * If mode == ADD: *target += update
   * If mode == MULT: *target *= update
   *
   * @param mode Mode to use for updating target
   * @param target Target to update
   * @param update Value to apply
   */
  void ApplyValueUpdate(ExecutionOperatingUnitFeatureUpdateMode mode, size_t *target, size_t update);

  /**
   * Update num_rows under a given mode and value
   * @param mode Mode to use for updating target
   * @param val Value to apply for the update
   */
  void UpdateNumRows(ExecutionOperatingUnitFeatureUpdateMode mode, size_t val) {
    ApplyValueUpdate(mode, &num_rows_, val);
  }

  /**
   * @return estimated number of output tuples
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
   * Update cardinality under a given mode and value
   * @param mode Mode to use for updating target
   * @param val Value to apply for the update
   */
  void UpdateCardinality(ExecutionOperatingUnitFeatureUpdateMode mode, size_t val) {
    ApplyValueUpdate(mode, &cardinality_, val);
  }

  /**
   * @return estimated cardinality
   */
  size_t GetCardinality() const { return cardinality_; }

  /**
   * Update num_concurrent under a given mode and value
   * @param mode Mode to use for updating target
   * @param val Value to apply for the update
   */
  void UpdateNumConcurrent(ExecutionOperatingUnitFeatureUpdateMode mode, size_t val) {
    ApplyValueUpdate(mode, &num_concurrent_, val);
  }

  /**
   * @return num concurrent
   */
  size_t GetNumConcurrent() const { return num_concurrent_; }

  /**
   * @return memory adjustment factor
   */
  double GetMemFactor() const {
    if (mem_factors_.empty()) return 1.0;

    double sum = 0.0;
    for (auto factor : mem_factors_) {
      sum += factor;
    }

    return sum / mem_factors_.size();
  }

  /**
   * Update num_loops under a given mode and value
   * @param mode Mode to use for updating target
   * @param val Value to apply for the update
   */
  void UpdateNumLoops(ExecutionOperatingUnitFeatureUpdateMode mode, size_t val) {
    ApplyValueUpdate(mode, &num_concurrent_, val);
  }

  /**
   * @return number of iterations
   */
  size_t GetNumLoops() const { return num_loops_; }

 private:
  /**
   * Set the estimated number of output tuples
   * @note only should be invoked by OperatingUnitRecorder or counters
   * @param num_rows Updated estimate
   */
  void SetNumRows(size_t num_rows) { num_rows_ = num_rows; }

  /**
   * Set the estimated cardinality
   * @note only should be invoked by OperatingUnitRecorder or counters
   * @param cardinality Updated cardinality
   */
  void SetCardinality(size_t cardinality) { cardinality_ = cardinality; }

  /*
   * Set the number of concurrent other tasks
   * @param num_concurrent number of concurent tasks
   */
  void SetNumConcurrent(size_t num_concurrent) { num_concurrent_ = num_concurrent; }

  /**
   * Set the estimated number of loops
   * @note only should be invoked by OperatingUnitRecorder or counters
   * @param num_loops Updated estimate
   */
  void SetNumLoops(size_t num_loops) { num_loops_ = num_loops; }

  /**
   * Set the mem factor
   * @note only should be invoked by OperatingUnitRecorder
   * @param mem_factor Updated mem_factor
   */
  void AddMemFactor(double mem_factor) { mem_factors_.emplace_back(mem_factor); }

  static std::atomic<execution::feature_id_t> feature_id_counter;

  execution::translator_id_t translator_id_;
  execution::feature_id_t feature_id_;
  ExecutionOperatingUnitType feature_;
  size_t num_rows_;
  size_t key_size_;
  size_t num_keys_;
  size_t cardinality_;
  std::vector<double> mem_factors_;
  size_t num_loops_;
  size_t num_concurrent_;
};

/**
 * Convenience typedef for a vector of features
 */
using ExecutionOperatingUnitFeatureVector = std::vector<ExecutionOperatingUnitFeature>;

/**
 * Class used to maintain information about a single feature's pipeline.
 * State is maintained in TLS during execution.
 */
class EXPORT ExecOUFeatureVector {
 public:
  /**
   * Pipeline ID
   */
  execution::pipeline_id_t pipeline_id_{execution::INVALID_PIPELINE_ID};

  /**
   * Features for a given pipeline
   *
   * This is a pointer because we need to be able to explicitly delete the
   * vector on all control flow paths. A standard std::vector may not be
   * properly cleaned up if execution encounters an "exception".
   */
  std::unique_ptr<execution::sql::MemPoolVector<ExecutionOperatingUnitFeature>> pipeline_features_ = nullptr;

  /**
   * Resets the feature vector state so it can be initialized again
   */
  void Reset() {
    pipeline_id_ = execution::INVALID_PIPELINE_ID;
    pipeline_features_ = nullptr;
  }

  /**
   * Function used to update a feature's metadata information
   * @param pipeline_id Pipeline Identifier
   * @param feature_id Feature Identifier
   * @param modifier Attribute to modify
   * @param mode Update mode to the value
   * @param val Value
   */
  void UpdateFeature(execution::pipeline_id_t pipeline_id, execution::feature_id_t feature_id,
                     ExecutionOperatingUnitFeatureAttribute modifier, ExecutionOperatingUnitFeatureUpdateMode mode,
                     uint32_t val);
};

/**
 * PipelineOperatingUnits manages the storage/association of specific pipeline
 * identifiers to the list of ExecutionOperatingUnitFeatures that are contained
 * within that given pipeline.
 */
class PipelineOperatingUnits {
 public:
  friend class noisepage::optimizer::IdxJoinTest_SimpleIdxJoinTest_Test;
  friend class noisepage::execution::compiler::test::CompilerTest_SimpleSeqScanTest_Test;
  friend class noisepage::execution::compiler::test::CompilerTest_SimpleSeqScanNonVecFilterTest_Test;
  friend class noisepage::execution::compiler::test::CompilerTest_SimpleSeqScanWithProjectionTest_Test;
  friend class noisepage::execution::compiler::test::CompilerTest_SimpleSeqScanWithParamsTest_Test;
  friend class noisepage::execution::compiler::test::CompilerTest_SimpleIndexScanTest_Test;
  friend class noisepage::execution::compiler::test::CompilerTest_SimpleIndexScanAscendingTest_Test;
  friend class noisepage::execution::compiler::test::CompilerTest_SimpleIndexScanLimitAscendingTest_Test;
  friend class noisepage::execution::compiler::test::CompilerTest_SimpleIndexScanDescendingTest_Test;
  friend class noisepage::execution::compiler::test::CompilerTest_SimpleIndexScanLimitDescendingTest_Test;
  friend class noisepage::execution::compiler::test::CompilerTest_SimpleAggregateTest_Test;
  friend class noisepage::execution::compiler::test::CompilerTest_CountStarTest_Test;
  friend class noisepage::execution::compiler::test::CompilerTest_SimpleSortTest_Test;
  friend class noisepage::execution::compiler::test::CompilerTest_SimpleAggregateHavingTest_Test;
  friend class noisepage::execution::compiler::test::CompilerTest_SimpleHashJoinTest_Test;
  friend class noisepage::execution::compiler::test::CompilerTest_MultiWayHashJoinTest_Test;
  friend class noisepage::execution::compiler::test::CompilerTest_SimpleNestedLoopJoinTest_Test;
  friend class noisepage::execution::compiler::test::CompilerTest_SimpleIndexNestedLoopJoinTest_Test;
  friend class noisepage::execution::compiler::test::CompilerTest_SimpleIndexNestedLoopJoinMultiColumnTest_Test;
  friend class noisepage::execution::compiler::test::CompilerTest_SimpleDeleteTest_Test;
  friend class noisepage::execution::compiler::test::CompilerTest_SimpleUpdateTest_Test;
  friend class noisepage::execution::compiler::test::CompilerTest_SimpleInsertTest_Test;
  friend class noisepage::execution::compiler::test::CompilerTest_InsertIntoSelectWithParamTest_Test;
  friend class noisepage::execution::compiler::test::CompilerTest_SimpleInsertWithParamsTest_Test;
  friend class noisepage::execution::compiler::test::CompilerTest_StaticDistinctAggregateTest_Test;
  friend class noisepage::runner::ExecutionRunners;

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
    NOISEPAGE_ASSERT(res.second, "Recording duplicate pipeline entry into PipelineOperatingUnits");
  }

  /**
   * Gets the features vector for a given pipeline identifier
   * @note asserts that pipeline identifier exists
   * @param pipeline pipeline identifier
   */
  const ExecutionOperatingUnitFeatureVector &GetPipelineFeatures(execution::pipeline_id_t pipeline) const {
    UNUSED_ATTRIBUTE auto itr = units_.find(pipeline);
    NOISEPAGE_ASSERT(itr != units_.end(), "Requested pipeline could not be found in PipelineOperatingUnits");
    return itr->second;
  }

  /**
   * Checks whether a certain pipeline exists
   * @param pipeline Pipeline Identifier
   * @return if exist or not
   */
  bool HasPipelineFeatures(execution::pipeline_id_t pipeline) const { return units_.find(pipeline) != units_.end(); }

 private:
  std::unordered_map<execution::pipeline_id_t, ExecutionOperatingUnitFeatureVector> units_{};
};

}  // namespace noisepage::selfdriving
