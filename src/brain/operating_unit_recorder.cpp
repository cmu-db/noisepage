#include "brain/operating_unit.h"
#include "brain/operating_unit_recorder.h"
#include "parser/expression_defs.h"
#include "planner/plannodes/plan_visitor.h"

namespace terrier::brain {

OperatingUnitFeatureVector OperatingUnitRecorder::RecordTranslators(
    const std::vector<std::unique_ptr<execution::compiler::OperatorTranslator>> &translators) {

  // Note that OperatorTranslators are roughly 1:1 with a plan node
  // As such, just emplace directly into feature vector
  std::vector<OperatingUnitFeature> results{};
  std::unordered_set<const planner::AbstractPlanNode *> plan_nodes;
  for (const auto &translator : translators) {
    // TODO(wz2): Populate actual num_rows/cardinality after #759
    auto feature_type = translator->GetFeatureType();
    auto num_rows = 0;
    auto cardinality = 0.0;
    plan_nodes.insert(translator->Op());
    results.emplace_back(feature_type, num_rows, cardinality);
  }

  for (auto *plan : plan_nodes) {
    // Consolidate the features based on OutputSchema
    plan->Accept(common::ManagedPointer<planner::PlanVisitor>(this));
  }

  for (auto &feature : features_) {
    results.emplace_back(std::move(feature.second));
  }

  return results;
}

}  // namespace terrier::brain
