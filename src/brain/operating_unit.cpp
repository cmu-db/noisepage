#include "brain/operating_unit.h"

namespace terrier::brain {

std::atomic<execution::feature_id_t> ExecutionOperatingUnitFeature::feature_id_counter{10000};  // arbitrary number

void ExecOUFeatureVector::UpdateFeature(execution::pipeline_id_t pipeline_id, execution::feature_id_t feature_id,
                                        ExecutionOperatingUnitFeatureAttribute modifier,
                                        ExecutionOperatingUnitFeatureUpdateMode mode, uint32_t val) {
  TERRIER_ASSERT(pipeline_id_ == pipeline_id, "Incorrect pipeline");
  TERRIER_ASSERT(pipeline_features_ != nullptr, "Pipeline Features cannot be null");
  size_t *value = nullptr;
  for (auto &feature : *pipeline_features_) {
    if (feature.GetFeatureId() == feature_id) {
      TERRIER_ASSERT(value == nullptr, "Duplicate feature found");
      switch (modifier) {
        case brain::ExecutionOperatingUnitFeatureAttribute::NUM_ROWS: {
          value = &feature.GetNumRows();
          break;
        }
        case brain::ExecutionOperatingUnitFeatureAttribute::CARDINALITY: {
          value = &feature.GetCardinality();
          break;
        }
        case brain::ExecutionOperatingUnitFeatureAttribute::NUM_LOOPS: {
          value = &feature.GetNumLoops();
          break;
        }
        default:
          TERRIER_ASSERT(false, "Invalid feature attribute.");
          return;
      }
    }
  }

  TERRIER_ASSERT(value != nullptr, "feature_id could not be found");
  if (value != nullptr) {
    switch (mode) {
      case brain::ExecutionOperatingUnitFeatureUpdateMode::SET: {
        *value = val;
        break;
      }
      case brain::ExecutionOperatingUnitFeatureUpdateMode::ADD: {
        *value = *value + val;
        break;
      }
      case brain::ExecutionOperatingUnitFeatureUpdateMode::MULT: {
        *value = *value * val;
        break;
      }
      default:
        TERRIER_ASSERT(false, "Invalid feature update mode");
    }
  }
}

}  // namespace terrier::brain
