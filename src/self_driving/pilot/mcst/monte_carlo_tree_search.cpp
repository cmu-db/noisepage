#include "self_driving/pilot/mcst/monte_carlo_tree_search.h"

#include <map>
#include <vector>

#include "common/managed_pointer.h"
#include "self_driving/pilot/action/generators/change_knob_action_generator.h"
#include "self_driving/pilot/action/generators/index_action_generator.h"
#include "self_driving/pilot_util.h"

namespace noisepage::selfdriving::pilot {

MonteCarloTreeSearch::MonteCarloTreeSearch(
    common::ManagedPointer<Pilot> pilot, common::ManagedPointer<selfdriving::WorkloadForecast> forecast,
    const std::vector<std::unique_ptr<planner::AbstractPlanNode>> &plans, uint64_t end_segment_index)
  : pilot_(pilot), forecast_(forecast), end_segment_index_(end_segment_index) {
  // populate action_map_, candidate_actions_
  IndexActionGenerator().GenerateActions(plans, pilot->settings_manager_, &action_map_, &candidate_actions_);
  ChangeKnobActionGenerator().GenerateActions(plans, pilot->settings_manager_, &action_map_, &candidate_actions_);
  // create root_
  auto later_cost = PilotUtil::ComputeCost(pilot, forecast, 0, end_segment_index);
  // root correspond to no action applied to any segment
  root_ = std::make_unique<TreeNode>(nullptr, static_cast<action_id_t>(NULL_ACTION), 0, later_cost);

}

void MonteCarloTreeSearch::BestAction(uint64_t simulation_number,
                                      std::vector<std::pair<const std::string, catalog::db_oid_t>> *best_action_seq) {
  for (auto i = 0; i < simulation_number; i++) {
    std::unordered_set<action_id_t> candidate_actions;
    for (auto action_id : candidate_actions_)
      candidate_actions.insert(action_id);
    auto vertex =
        TreeNode::Selection(common::ManagedPointer(root_), pilot_, action_map_, &candidate_actions);
    vertex->ChildrenRollout(pilot_, forecast_, 0, end_segment_index_, action_map_, candidate_actions);
    vertex->BackPropogate(pilot_, action_map_);
  }
  // return the best action at root
  auto curr_node = common::ManagedPointer(root_);
  while(!curr_node->IsLeaf()) {
    auto best_child = curr_node->BestSubtree();
    best_action_seq->emplace_back(action_map_.at(best_child->GetCurrentAction())->GetSQLCommand(),
                                  action_map_.at(best_child->GetCurrentAction())->GetDatabaseOid());
    std::cout << action_map_.at(best_child->GetCurrentAction())->GetSQLCommand() << std::endl;
    curr_node = best_child;
  }

}

}  // namespace noisepage::selfdriving::pilot
