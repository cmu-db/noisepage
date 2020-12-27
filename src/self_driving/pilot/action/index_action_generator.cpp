#include "self_driving/pilot/action/index_action_generator.h"

#include "self_driving/pilot/action/abstract_action.h"
#include "planner/plannodes/index_scan_plan_node.h"

namespace noisepage::selfdriving::pilot {

std::pair<std::vector<std::unique_ptr<AbstractAction>>, std::vector<action_id_t>>
IndexActionGenerator::GenerateIndexActions(const std::vector<std::unique_ptr<planner::AbstractPlanNode>> &plans) {
  // Clear previous actions if any
  actions_.clear();
  candidate_action_ids_.clear();

  // Find the "missing" index for each plan and generate the corresponding actions
  for (auto &plan : plans) {
    // Currently using a heuristic to find the scan predicates that are not fully covered by an existing index, and
    // generate actions to build indexes that cover the full predicates
    FindMissingIndex(plan.get());
  }

  return std::make_pair(std::move(actions_), std::move(candidate_action_ids_));

}

void IndexActionGenerator::FindMissingIndex(const planner::AbstractPlanNode *plan) {
  // Visit all the child nodes
  auto children = plan->GetChildren();
  for (auto child : children) FindMissingIndex(child.Get());

  auto plan_type = plan->GetPlanNodeType();
  if (plan_type == planner::PlanNodeType::SEQSCAN || plan_type == planner::PlanNodeType::INDEXSCAN) {
    auto predicate = reinterpret_cast<const planner::AbstractScanPlanNode*>(plan)->GetScanPredicate();
    if (predicate == nullptr) return;

    // The index already covers all the indexable columns
    if (plan_type == planner::PlanNodeType::INDEXSCAN && reinterpret_cast<const planner::IndexScanPlanNode*>(plan)
                                                             ->GetCoverAllColumns()) return;

    // Find the "missing" index
  }
}

}  // namespace noisepage::selfdriving::pilot
