
#include <random>
#include <cmath>

#include "self_driving/pilot/mcst/tree_node.h"
#include "self_driving/pilot/action/abstract_action.h"
#include "self_driving/pilot/pilot.h"
#include "self_driving/pilot_util.h"
#include "self_driving/forecast/workload_forecast.h"

#define EPSILON 1e-3

namespace noisepage::selfdriving::pilot {

TreeNode::TreeNode(common::ManagedPointer<TreeNode> parent, action_id_t current_action,
                   const uint64_t current_segment_cost, uint64_t later_segments_cost)
  : is_leaf_{true}, depth_(parent == nullptr ? 0 : parent->depth_ + 1), current_action_(current_action),
      ancestor_cost_(current_segment_cost + (parent == nullptr ? 0 : parent->ancestor_cost_)),
      parent_(parent), number_of_visits_{1} {

  if (parent != nullptr)
    parent->is_leaf_ = false;
  cost_ = ancestor_cost_ + later_segments_cost;
}

common::ManagedPointer<TreeNode> TreeNode::BestSubtree() {
  NOISEPAGE_ASSERT(!is_leaf_, "Trying to return best action on a leaf node");
  // Get child of least cost
  NOISEPAGE_ASSERT(children_.size() > 0, "Trying to return best action for unexpanded nodes");
  auto best_child = common::ManagedPointer(children_[0]);
  for (auto &child : children_)
    if (child->cost_ < best_child->cost_)
      best_child = common::ManagedPointer(child);
  return best_child;
}

void TreeNode::UpdateCostAndVisits(uint64_t num_expansion, uint64_t leaf_cost, uint64_t expanded_cost) {
  // compute cost as average of the leaves in its subtree weighted by the number of visits
  // Here we add the number of successors expanded in the previous rollout and also subtract 1
  // because the cost of the expanded leaf was invalidated
  auto new_num_visits = num_expansion - 1 + number_of_visits_;

  cost_ = (number_of_visits_ / new_num_visits) * cost_
          - leaf_cost / new_num_visits
          + (num_expansion / new_num_visits) * expanded_cost;
  number_of_visits_ = new_num_visits;
}

common::ManagedPointer<TreeNode> TreeNode::SampleChild() {
  // compute max of children's cost
  uint64_t highest = 0;
  for (auto &child : children_)
    highest = std::max(child->cost_, highest);

  // sample based on cost and num of visits of children
  std::vector<double> children_weights;
  for (auto &child : children_) {
    // Adopted from recommended formula in https://en.wikipedia.org/wiki/Monte_Carlo_tree_search#Exploration_and_exploitation
    // The first additive term was changed to be the inverse of a normalized value, since smaller cost is preferred
    auto child_obj = std::pow((highest + EPSILON) / (child->cost_ + EPSILON), 2) +
                     std::sqrt(2 * std::log(number_of_visits_) / child->number_of_visits_ );
    children_weights.push_back(child_obj);
  }
  std::discrete_distribution<double> children_dist(children_weights.begin(), children_weights.end());
  auto device = std::mt19937{std::random_device{}()};
  return common::ManagedPointer(children_.at(children_dist(device)));
}

common::ManagedPointer<TreeNode> TreeNode::Selection(
    common::ManagedPointer<TreeNode> root,
    common::ManagedPointer<Pilot> pilot,
    const std::map<action_id_t, std::unique_ptr<AbstractAction>> &action_map,
    std::unordered_set<action_id_t> *candidate_actions) {
  common::ManagedPointer<TreeNode> curr = root;
  while(!curr->is_leaf_) {
    curr = curr->SampleChild();
    for (auto invalid_action: action_map.at(curr->current_action_)->GetInvalidatedActions()) {
      candidate_actions->erase(invalid_action);
    }
    for (auto enabled_action : action_map.at(curr->current_action_)->GetEnabledActions()) {
      candidate_actions->insert(enabled_action);
    }
    PilotUtil::ApplyAction(pilot, action_map.at(curr->current_action_)->GetSQLCommand(), action_map.at(curr->current_action_)->GetDatabaseOid());
  }
  return curr;
}

void TreeNode::ChildrenRollout(common::ManagedPointer<Pilot> pilot,
                               common::ManagedPointer<selfdriving::WorkloadForecast> forecast,
                               uint64_t tree_start_segment_index, uint64_t tree_end_segment_index,
                               const std::map<action_id_t, std::unique_ptr<AbstractAction>> &action_map,
                               const std::unordered_set<action_id_t> &candidate_actions) {
  auto start_segment_index = tree_start_segment_index + depth_;
  auto end_segment_index = tree_end_segment_index;

  for (const auto &action_id : candidate_actions) {
    // expand each action not yet applied
    if (!action_map.at(action_id)->IsValid() || action_map.at(action_id)->GetSQLCommand() == "set compiled_query_execution = 'true';") continue;
    PilotUtil::ApplyAction(pilot, action_map.at(action_id)->GetSQLCommand(), action_map.at(action_id)->GetDatabaseOid());

    uint64_t child_segment_cost =
        PilotUtil::ComputeCost(pilot, forecast, start_segment_index, start_segment_index);
    uint64_t later_segments_cost =
        PilotUtil::ComputeCost(pilot, forecast, start_segment_index + 1, end_segment_index);

    children_.push_back(std::make_unique<TreeNode>(
        common::ManagedPointer(this), action_id, child_segment_cost, later_segments_cost));

    // apply one reverse action to undo the above
    auto rev_actions = action_map.at(action_id)->GetReverseActions();
    PilotUtil::ApplyAction(pilot, action_map.at(rev_actions[0])->GetSQLCommand(), action_map.at(rev_actions[0])->GetDatabaseOid());
  }
}

void TreeNode::BackPropogate(common::ManagedPointer<Pilot> pilot,
                             const std::map<action_id_t, std::unique_ptr<AbstractAction>> &action_map) {
  auto curr = common::ManagedPointer(this);
  auto leaf_cost = cost_;
  auto expanded_cost = ComputeCostFromChildren();

  auto num_expansion = children_.size();
  while(curr != nullptr && curr->parent_ != nullptr) {
    auto rev_action = action_map.at(curr->current_action_)->GetReverseActions()[0];
    PilotUtil::ApplyAction(pilot, action_map.at(rev_action)->GetSQLCommand(), action_map.at(rev_action)->GetDatabaseOid());
    // All ancestors of the expanded leaf need to updated with a weight increase of num_expansion, and a new weight
    curr->UpdateCostAndVisits(num_expansion, leaf_cost, expanded_cost);
    curr = curr->parent_;
  }
}

}  // namespace noisepage::selfdriving::pilot
