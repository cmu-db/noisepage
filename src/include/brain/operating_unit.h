#pragma once
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>
#include "brain/brain_defs.h"
#include "execution/exec_defs.h"

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
 *   Estimated number of tuples
 *   Estimated cardinality
 */
class ExecutionOperatingUnitFeature {
  friend class PipelineOperatingUnits;
  friend class OperatingUnitRecorder;

 public:
  /**
   * Constructor for ExecutionOperatingUnitFeature
   * @param feature Type
   * @param num_rows Estimated number of output tuples
   * @param cardinality Estimated cardinality
   */
  ExecutionOperatingUnitFeature(ExecutionOperatingUnitType feature, size_t num_rows, double cardinality)
      : feature_(feature), num_rows_(num_rows), cardinality_(cardinality) {}

  /**
   * @returns type
   */
  ExecutionOperatingUnitType GetExecutionOperatingUnitType() const { return feature_; }

  /**
   * @returns estimated number of output tuples
   */
  size_t GetNumRows() const { return num_rows_; }

  /**
   * @returns estimated cardinality
   */
  double GetCardinality() const { return cardinality_; }

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
  void SetCardinality(double cardinality) { cardinality_ = cardinality; }

  ExecutionOperatingUnitType feature_;
  size_t num_rows_;
  double cardinality_;
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
