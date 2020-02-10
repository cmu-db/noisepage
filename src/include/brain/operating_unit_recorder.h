#pragma once

#include <vector>
#include <unordered_map>

#include "common/managed_pointer.h"
#include "planner/plannodes/plan_visitor.h"
#include "execution/compiler/operator/operator_translator.h"
#include "brain/operating_unit.h"

namespace terrier::brain {

class OperatingUnitRecorder : planner::PlanVisitor {
 public:
  OperatingUnitFeatureVector RecordTranslators(const std::vector<std::unique_ptr<execution::compiler::OperatorTranslator>> &translators);

 private:
  std::unordered_map<OperatingUnitFeatureType, OperatingUnitFeature> features_;
};

}  // namespace terrier::brain
