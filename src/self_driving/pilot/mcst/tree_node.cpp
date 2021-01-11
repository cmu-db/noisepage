
#include "self_driving/pilot/mcst/tree_node.h"
#include "self_driving/pilot/action/abstract_action.h"
#include "self_driving/pilot/pilot.h"
#include "self_driving/pilot_util.h"
#include "self_driving/forecast/workload_forecast.h"


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

common::ManagedPointer<TreeNode> TreeNode::SampleChild() {
  // TODO: Sample based on cost of children
  return common::ManagedPointer<TreeNode>(children_[0]);
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
