#pragma once
#include <memory>
#include <vector>
#include "brain/brain_defs.h"
#include "execution/vm/module.h"
#include "execution/exec_defs.h"

namespace terrier::brain {

class OperatingUnitFeature {
 public:
  OperatingUnitFeature(OperatingUnitFeatureType feature, execution::vm::ExecutionMode exec_mode, size_t num_rows,
                       double cardinality)
      : feature_(feature), exec_mode_(exec_mode), num_rows_(num_rows), cardinality_(cardinality) {}

  OperatingUnitFeatureType GetOperatingUnitFeatureType() const { return feature_; }
  execution::vm::ExecutionMode GetExecutionMode() const { return exec_mode_; }
  size_t GetNumRows() const { return num_rows_; }
  double GetCardinality() const { return cardinality_; }

 private:
  OperatingUnitFeatureType feature_;
  execution::vm::ExecutionMode exec_mode_;
  size_t num_rows_;
  double cardinality_;
};

class OperatingUnit {
 public:
  OperatingUnit(execution::pipeline_id_t pipeline_idx, std::vector<OperatingUnitFeature> &&features)
      : pipeline_idx_(pipeline_idx), features_(features) {}

  execution::pipeline_id_t GetPipelineIdx() const { return pipeline_idx_; }
  const std::vector<OperatingUnitFeature> &GetFeatures() const { return features_; }

 private:
  execution::pipeline_id_t pipeline_idx_;
  std::vector<OperatingUnitFeature> features_;
};

class OperatingUnitsStorage {
 public:
  OperatingUnitsStorage() {}

  void RecordOperatingUnit(execution::pipeline_id_t pipeline, OperatingUnit &&unit) {
    UNUSED_ATTRIBUTE auto res = units_.insert(std::make_pair(pipeline, std::move(unit)));
    TERRIER_ASSERT(res.second, "Recording duplicate pipeline entry into OperatingUnitsStorage");
  }

  const OperatingUnit &GetPipeline(execution::pipeline_id_t pipeline) const {
    UNUSED_ATTRIBUTE auto itr = units_.find(pipeline);
    TERRIER_ASSERT(itr != units_.end(), "Requested pipeline could not be found in OperatingUnitsStorage");
    return itr->second;
  }

 private:
  std::unordered_map<execution::pipeline_id_t, OperatingUnit> units_{};
};

}  // namespace terrier::brain
