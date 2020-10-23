#include "brain/operating_unit.h"

namespace terrier::brain {

std::atomic<execution::feature_id_t> ExecutionOperatingUnitFeature::feature_id_counter{10000};  // arbitrary number

void ExecutionOperatingUnitFeature::ApplyValueUpdate(ExecutionOperatingUnitFeatureUpdateMode mode, size_t *target,
                                                     size_t update) {
  switch (mode) {
    case brain::ExecutionOperatingUnitFeatureUpdateMode::SET: {
      *target = update;
      break;
    }
    case brain::ExecutionOperatingUnitFeatureUpdateMode::ADD: {
      *target = *target + update;
      break;
    }
    case brain::ExecutionOperatingUnitFeatureUpdateMode::MULT: {
      *target = *target * update;
      break;
    }
    default:
      TERRIER_ASSERT(false, "Invalid feature update mode");
  }
}

void ExecOUFeatureVector::UpdateFeature(execution::pipeline_id_t pipeline_id, execution::feature_id_t feature_id,
                                        ExecutionOperatingUnitFeatureAttribute modifier,
                                        ExecutionOperatingUnitFeatureUpdateMode mode, uint32_t val) {
  TERRIER_ASSERT(pipeline_id_ == pipeline_id, "Incorrect pipeline");
  TERRIER_ASSERT(pipeline_features_ != nullptr, "Pipeline Features cannot be null");

  UNUSED_ATTRIBUTE bool did_find = false;
  for (auto &feature : *pipeline_features_) {
    if (feature.GetFeatureId() == feature_id) {
      TERRIER_ASSERT(!did_find, "Duplicate feature found");
      switch (modifier) {
        case brain::ExecutionOperatingUnitFeatureAttribute::NUM_ROWS: {
          feature.UpdateNumRows(mode, val);
          did_find = true;
          break;
        }
        case brain::ExecutionOperatingUnitFeatureAttribute::CARDINALITY: {
          feature.UpdateCardinality(mode, val);
          did_find = true;
          break;
        }
        case brain::ExecutionOperatingUnitFeatureAttribute::NUM_LOOPS: {
          feature.UpdateNumConcurrent(mode, val);
          did_find = true;
          break;
        }
        default:
          TERRIER_ASSERT(false, "Invalid feature attribute.");
          return;
      }
    }
  }

  TERRIER_ASSERT(did_find, "No matching feature was found");
}

}  // namespace terrier::brain
