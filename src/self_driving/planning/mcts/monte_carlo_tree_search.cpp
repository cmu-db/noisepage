#include "self_driving/planning/mcts/monte_carlo_tree_search.h"

#include <vector>

#include "common/managed_pointer.h"
#include "loggers/selfdriving_logger.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "self_driving/planning/action/generators/change_knob_action_generator.h"
#include "self_driving/planning/action/generators/index_action_generator.h"
#include "self_driving/planning/pilot.h"
#include "self_driving/planning/pilot_util.h"

namespace noisepage::selfdriving::pilot {

MonteCarloTreeSearch::MonteCarloTreeSearch(const PlanningContext &planning_context,
                                           common::ManagedPointer<selfdriving::WorkloadForecast> forecast,
                                           uint64_t end_segment_index, bool use_min_cost)
    : planning_context_(planning_context),
      forecast_(forecast),
      end_segment_index_(end_segment_index),
      use_min_cost_(use_min_cost) {
  std::vector<std::unique_ptr<planner::AbstractPlanNode>> plans;
  // vector of query plans that the search tree is responsible for
  PilotUtil::GetQueryPlans(planning_context_, common::ManagedPointer(forecast_), end_segment_index, &plans);

  // populate action_map_, candidate_actions_
  IndexActionGenerator().GenerateActions(plans, planning_context_.GetSettingsManager(), &action_map_,
                                         &candidate_actions_);
  ChangeKnobActionGenerator().GenerateActions(plans, planning_context_.GetSettingsManager(), &action_map_,
                                              &candidate_actions_);

  for (const auto &it UNUSED_ATTRIBUTE : action_map_) {
    SELFDRIVING_LOG_INFO("Generated action: ID {} Command {}", it.first, it.second->GetSQLCommand());
  }

  // Estimate the create index action costs
  for (auto &[action_id, action] : action_map_) {
    if (action->GetActionType() == ActionType::CREATE_INDEX) {
      auto create_action = reinterpret_cast<CreateIndexAction *>(action.get());
      auto drop_action = reinterpret_cast<DropIndexAction *>(action_map_.at(action->GetReverseActions().at(0)).get());
      PilotUtil::EstimateCreateIndexAction(planning_context_, create_action, drop_action);
    }
  }

  // create root_
  auto later_cost = PilotUtil::ComputeCost(planning_context_, forecast, 0, end_segment_index);
  ActionState action_state;
  action_state.SetIntervals(0, end_segment_index);
  // root correspond to no action applied to any segment
  root_ = std::make_unique<TreeNode>(nullptr, static_cast<action_id_t>(NULL_ACTION), 0, 0, later_cost,
                                     planning_context_.GetMemoryInfo().initial_memory_bytes_, action_state);
  // TODO(lin): actually using the cost map during the search to reduce computation
  action_state_cost_map_.emplace(std::make_pair(std::move(action_state), later_cost));
}

void MonteCarloTreeSearch::RunSimulation(uint64_t simulation_number, uint64_t memory_constraint) {
  for (uint64_t i = 0; i < simulation_number; i++) {
    std::unordered_set<action_id_t> candidate_actions;
    for (auto action_id : candidate_actions_) candidate_actions.insert(action_id);
    auto vertex = TreeNode::Selection(common::ManagedPointer(root_), planning_context_, action_map_, &candidate_actions,
                                      end_segment_index_);

    vertex->ChildrenRollout(planning_context_, forecast_, levels_to_plan_.at(vertex->GetDepth()), end_segment_index_,
                            action_map_, candidate_actions, memory_constraint);
    vertex->BackPropogate(planning_context_, action_map_, use_min_cost_);
  }
}

void MonteCarloTreeSearch::BestAction(std::vector<std::vector<pilot::ActionTreeNode>> *best_action_seq, size_t topk) {
  auto curr_node = common::ManagedPointer(root_);
  std::vector<action_id_t> reversals;
  while (!curr_node->IsLeaf()) {
    std::vector<ActionTreeNode> top;
    std::vector<common::ManagedPointer<TreeNode>> order = curr_node->BestSubtreeOrdering();
    NOISEPAGE_ASSERT(!order.empty(), "TreeNode should have some children");

    for (size_t i = 0; i < order.size() && i < topk; i++) {
      auto child = order[i];
      auto action = child->GetCurrentAction();
      auto &action_info = action_map_.at(action);
      top.emplace_back(curr_node->GetTreeNodeId(), child->GetTreeNodeId(), action, child->GetCost(),
                       action_info->GetDatabaseOid(), action_info->GetSQLCommand(), child->GetActionStartSegmentIndex(),
                       child->GetActionPlanEndIndex());
    }

    best_action_seq->emplace_back(std::move(top));
    curr_node = order[0];

    // Apply the root action. This is because some actions (i.e., ChangeKnobActions) are deltas
    // rather than absolute values. Actions are applied as "what-if" since they will be reversed
    // afterwards once the traversal ends.
    auto action = curr_node->GetCurrentAction();
    auto &action_info = action_map_.at(action);
    PilotUtil::ApplyAction(planning_context_, action_info->GetSQLCommand(), action_info->GetDatabaseOid(), true);
    NOISEPAGE_ASSERT(!action_info->GetReverseActions().empty(), "Action should have reverse");
    reversals.push_back(action_info->GetReverseActions()[0]);
  }

  for (auto it = reversals.rbegin(); it != reversals.rend(); it++) {
    auto action = *it;
    auto &action_info = action_map_.at(action);
    PilotUtil::ApplyAction(planning_context_, action_info->GetSQLCommand(), action_info->GetDatabaseOid(), true);
  }
}

}  // namespace noisepage::selfdriving::pilot
