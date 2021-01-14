
#include <map>
#include <vector>

#include "common/managed_pointer.h"
#include "self_driving/pilot/action/generators/index_action_generator.h"
#include "self_driving/pilot/action/generators/change_knob_action_generator.h"
#include "self_driving/pilot/mcst/monte_carlo_search_tree.h"
#include "self_driving/pilot_util.h"

namespace noisepage::selfdriving::pilot {

MonteCarloSearchTree::MonteCarloSearchTree(
    common::ManagedPointer<Pilot> pilot, common::ManagedPointer<selfdriving::WorkloadForecast> forecast,
    const std::vector<std::unique_ptr<planner::AbstractPlanNode>> &plans, uint64_t start_segment_index,
    uint64_t end_segment_index)
  : pilot_(pilot), forecast_(forecast), start_segment_index_(start_segment_index),
    end_segment_index_(end_segment_index) {
  // populate action_map_, candidate_actions_
  IndexActionGenerator().GenerateActions(plans, pilot->settings_manager_, &action_map_, &candidate_actions_);
  ChangeKnobActionGenerator().GenerateActions(plans, pilot->settings_manager_, &action_map_, &candidate_actions_);
  // create root_
  auto later_cost = PilotUtil::ComputeCost(pilot, forecast, start_segment_index, end_segment_index);
  // root correspond to no action applied to any segment
  root_ = std::make_unique<TreeNode>(nullptr, static_cast<action_id_t>(NULL_ACTION), 0, later_cost);

  // preprocess db_oids, get all db_oids starting with the current segment until the end of planning horizon
  std::vector<uint64_t> curr_oids;
  for (auto idx = end_segment_index_; idx >= start_segment_index_; idx--) {
    for (auto oid : pilot->forecast_->forecast_segments_[idx].GetDBOids()) {
      if (std::find(curr_oids.begin(), curr_oids.end(), oid) == curr_oids.end())
        curr_oids.push_back(oid);
    }
    db_oids_.push_back(curr_oids);
  }
  std::reverse(db_oids_.begin(), db_oids_.end());
}

const std::string MonteCarloSearchTree::BestAction(uint64_t simulation_number) {
  for (auto i = 0; i < simulation_number; i++) {
    std::unordered_set<action_id_t> candidate_actions;
    for (auto action_id : candidate_actions_)
      candidate_actions.insert(action_id);

    auto vertex =
        TreeNode::Selection(common::ManagedPointer(root_), pilot_, db_oids_, action_map_, &candidate_actions);
    vertex->ChildrenRollout(pilot_, forecast_, start_segment_index_, end_segment_index_, db_oids_,
                            action_map_, candidate_actions);
    vertex->BackPropogate(pilot_, db_oids_, action_map_);
  }
  // return the best action at root
  auto best_action_id = TreeNode::BestSubtree(common::ManagedPointer(root_));
  return action_map_.at(best_action_id)->GetSQLCommand();
}

}  // namespace noisepage::selfdriving::pilot
