
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
    const std::vector<std::unique_ptr<planner::AbstractPlanNode>> &plans, uint64_t action_planning_horizon,
    uint64_t simulation_number, uint64_t start_segment_index)
  : pilot_(pilot), forecast_(forecast), start_segment_index_(start_segment_index),
      action_planning_horizon_(action_planning_horizon), simulation_number_(simulation_number)
{
  // populate action_map_, candidate_actions_
  IndexActionGenerator().GenerateActions(plans, pilot->settings_manager_, &action_map_, &candidate_actions_);
  ChangeKnobActionGenerator().GenerateActions(plans, pilot->settings_manager_, &action_map_, &candidate_actions_);
  // create root_
  root_ = std::make_unique<TreeNode>(nullptr, static_cast<action_id_t>(NULL_ACTION), 0);

  //preprocess db_oids, get all db_oids starting with the current segment until the end of planning horizon
  std::vector<uint64_t> curr_oids;
  db_oids_.reserve(action_planning_horizon_);
  for (auto idx = action_planning_horizon_ - 1; idx >= 0; idx--) {
    for (auto oid : pilot->forecast_->forecast_segments_[start_segment_index + idx].GetDBOids()) {
      if (std::find(curr_oids.begin(), curr_oids.end(), oid) == curr_oids.end())
        curr_oids.push_back(oid);
    }
    db_oids_[idx] = curr_oids;
  }
}

const std::string MonteCarloSearchTree::BestAction(std::map<pilot::action_id_t, std::unique_ptr<pilot::AbstractAction>> *best_action_map,
                                                   std::vector<pilot::action_id_t> *best_action_seq) {
  for (auto i = 0; i < simulation_number_; i++) {
    std::unordered_set<action_id_t> candidate_actions;
    for (auto action_id : candidate_actions_)
      candidate_actions.insert(action_id);

    auto vertex = Selection(&candidate_actions);
    vertex->ChildrenRollout(pilot_, forecast_, start_segment_index_, start_segment_index_ + vertex->GetDepth(),
                            db_oids_, action_map_, candidate_actions);
    BackPropogate(vertex);
  }
  // return the best action at root
  auto best_action = root_->BestChild()->GetCurrentAction();
  best_action_map->emplace(best_action, std::move(action_map_.at(best_action)));
  best_action_seq->emplace_back(best_action);
  return action_map_.at(best_action)->GetSQLCommand();
}

common::ManagedPointer<TreeNode> MonteCarloSearchTree::Selection(
    std::unordered_set<action_id_t> *candidate_actions) {
  common::ManagedPointer<TreeNode> curr = common::ManagedPointer(root_);
  while(!curr->IsLeaf()) {
    curr = curr->SampleChild();
    candidate_actions->erase(curr->GetCurrentAction());
    for (auto rev_action : action_map_.at(curr->GetCurrentAction())->GetReverseActions()) {
      candidate_actions->insert(rev_action);
    }
    PilotUtil::ApplyAction(pilot_, db_oids_[curr->GetDepth()],
                           action_map_.at(curr->GetCurrentAction())->GetSQLCommand());
  }
  return curr;
}

void MonteCarloSearchTree::BackPropogate(common::ManagedPointer<TreeNode> node) {
  auto curr = node;
  while(curr->GetParent() != nullptr) {
    auto rev_action = action_map_.at(curr->GetCurrentAction())->GetReverseActions()[0];
    PilotUtil::ApplyAction(pilot_, db_oids_[curr->GetDepth()],
                           action_map_.at(rev_action)->GetSQLCommand());
    // TODO: Update cost of curr
    curr->UpdateVisits(node->GetNumberOfChildren());

    curr = curr->GetParent();
  }
}

}  // namespace noisepage::selfdriving::pilot
