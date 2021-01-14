
#include <random>
#include <cmath>

#include "self_driving/pilot/mcst/tree_node.h"
#include "self_driving/pilot/action/abstract_action.h"
#include "self_driving/pilot/pilot.h"
#include "self_driving/pilot_util.h"
#include "self_driving/forecast/workload_forecast.h"

#define EPSILON 1e-3

namespace noisepage::selfdriving::pilot {

/**
 *
 */
TreeNode::TreeNode(common::ManagedPointer<TreeNode> parent, action_id_t current_action, uint64_t cost)
  : is_leaf_{true}, number_of_visits_{1}, parent_(parent), current_action_(current_action), cost_(cost) {
  if (parent == nullptr) {
    depth_ = 0;
  } else {
    depth_ = parent->depth_ + 1;
    parent->is_leaf_ = false;
  }
}

common::ManagedPointer<TreeNode> TreeNode::BestChild() {
  // Get child of least cost
  NOISEPAGE_ASSERT(children_.size() > 0, "calling best child method on non-expanded nodes");
  auto best_child = common::ManagedPointer(children_[0]);
  for (auto &child : children_)
    if (child->cost_ < best_child->cost_)
      best_child = common::ManagedPointer(child);
  return best_child;
}

void TreeNode::UpdateCostAndVisits(uint64_t num_expansion, uint64_t leaf_cost, uint64_t new_cost) {
  // compute cost as average of its children weighted by number of visits
  auto new_num_visits = num_expansion - 1 + number_of_visits_;
  cost_ = (number_of_visits_ / new_num_visits) * cost_
          - leaf_cost / new_num_visits
          + (num_expansion / new_num_visits) * new_cost;
  number_of_visits_ = new_num_visits;
}

common::ManagedPointer<TreeNode> TreeNode::SampleChild() {
  // compute max of children's cost
  uint64_t highest = 0;
  for (auto &child : children_)
    highest = std::max(child->cost_, highest);

  // sample based on cost and num of visits of children
  std::vector<common::ManagedPointer<TreeNode>> selected_children, out;
  uint64_t best_value = 0;
  for (auto &child : children_) {
    auto child_obj = std::pow((highest + EPSILON) / (child->cost_ + EPSILON), 2) +
                     std::sqrt(2 * std::log(number_of_visits_) / child->number_of_visits_ );
    if (child_obj > best_value) {
      best_value = child_obj;
      selected_children = {};
      selected_children.push_back(common::ManagedPointer(child));
    } else if (child_obj == best_value) {
      selected_children.push_back(common::ManagedPointer(child));
    }
  }

  std::sample(selected_children.begin(), selected_children.end(), std::back_inserter(out),
              1, std::mt19937{std::random_device{}()});
  return out[0];
}

void TreeNode::ChildrenRollout(common::ManagedPointer<Pilot> pilot,
                               common::ManagedPointer<selfdriving::WorkloadForecast> forecast,
                               uint64_t start_segment_index,
                               uint64_t end_segment_index,
                               const std::vector<std::vector<uint64_t>> &db_oids,
                               const std::map<action_id_t, std::unique_ptr<AbstractAction>> &action_map,
                               const std::unordered_set<action_id_t> &candidate_actions) {

  for (const auto &action_id : candidate_actions) {
    // expand each action not yet applied
    PilotUtil::ApplyAction(pilot, db_oids[depth_], action_map.at(action_id)->GetSQLCommand());

    uint64_t child_cost = PilotUtil::ComputeCost(pilot, forecast, start_segment_index, end_segment_index);
    children_.push_back(std::make_unique<TreeNode>(common::ManagedPointer(this), action_id, child_cost));

    // apply one reverse action to undo the above
    auto rev_actions = action_map.at(action_id)->GetReverseActions();
    PilotUtil::ApplyAction(pilot, db_oids[depth_], action_map.at(rev_actions[0])->GetSQLCommand());
  }
}

}  // namespace noisepage::selfdriving::pilot
