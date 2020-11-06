#include "brain/operating_unit.h"

namespace noisepage::brain {

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
      NOISEPAGE_ASSERT(false, "Invalid feature update mode");
  }
}

void ExecOUFeatureVector::UpdateFeature(execution::pipeline_id_t pipeline_id, execution::feature_id_t feature_id,
                                        ExecutionOperatingUnitFeatureAttribute modifier,
                                        ExecutionOperatingUnitFeatureUpdateMode mode, uint32_t val) {
  NOISEPAGE_ASSERT(pipeline_id_ == pipeline_id, "Incorrect pipeline");
  NOISEPAGE_ASSERT(pipeline_features_ != nullptr, "Pipeline Features cannot be null");

  UNUSED_ATTRIBUTE bool did_find = false;
  for (auto &feature : *pipeline_features_) {
    if (feature.GetFeatureId() == feature_id) {
      NOISEPAGE_ASSERT(!did_find, "Duplicate feature found");
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
          NOISEPAGE_ASSERT(false, "Invalid feature attribute.");
          return;
      }
    }
  }

  NOISEPAGE_ASSERT(did_find, "No matching feature was found");
}

}  // namespace noisepage::brain
