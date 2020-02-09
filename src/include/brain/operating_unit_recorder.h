#pragma once

#include <vector>
#include <unordered_map>

#include "common/managed_pointer.h"
#include "execution/compiler/operator/operator_translator.h"
#include "brain/operating_unit.h"

namespace terrier::brain {

class OperatingUnitRecorder {
 public:
  OperatingUnitRecorder() {}

  void RecordFromTranslator(common::ManagedPointer<execution::compiler::OperatorTranslator> translator);

  /**
   * Destructively gets the features vector.
   * Utilizes the move operator
   * @return features vector
   */
  std::vector<OperatingUnitFeature> ReleaseFeatures() { return std::move(features); }

 private:
  std::vector<OperatingUnitFeature> features;
  std::unordered_map<OperatingUnitFeatureType, uint32_t> feature_idx;
};

}  // namespace terrier::brain
