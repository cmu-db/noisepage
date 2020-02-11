#pragma once
#include <memory>
#include <vector>
#include "brain/brain_defs.h"
#include "execution/exec_defs.h"

namespace terrier::brain {

class OperatingUnitRecorder;

class OperatingUnitFeature {
 friend class OperatingUnits;
 friend class OperatingUnitRecorder;

 public:
  OperatingUnitFeature(OperatingUnitFeatureType feature, size_t num_rows, double cardinality)
      : feature_(feature), num_rows_(num_rows), cardinality_(cardinality) {}

  OperatingUnitFeatureType GetOperatingUnitFeatureType() const { return feature_; }
  size_t GetNumRows() const { return num_rows_; }
  double GetCardinality() const { return cardinality_; }

 private:
  void SetNumRows(size_t num_rows) { num_rows_ = num_rows; }
  void SetCardinality(double cardinality) { cardinality_ = cardinality; }

  OperatingUnitFeatureType feature_;
  size_t num_rows_;
  double cardinality_;
};

using OperatingUnitFeatureVector = std::vector<OperatingUnitFeature>;

class OperatingUnits {
 public:
  OperatingUnits() {}

  void RecordOperatingUnit(execution::pipeline_id_t pipeline, OperatingUnitFeatureVector &&features) {
    UNUSED_ATTRIBUTE auto res = units_.insert(std::make_pair(pipeline, std::move(features)));
    TERRIER_ASSERT(res.second, "Recording duplicate pipeline entry into OperatingUnitsStorage");
  }

  const OperatingUnitFeatureVector &GetPipelineFeatures(execution::pipeline_id_t pipeline) const {
    UNUSED_ATTRIBUTE auto itr = units_.find(pipeline);
    TERRIER_ASSERT(itr != units_.end(), "Requested pipeline could not be found in OperatingUnitsStorage");
    return itr->second;
  }

 private:
  std::unordered_map<execution::pipeline_id_t, OperatingUnitFeatureVector> units_{};
};

}  // namespace terrier::brain
