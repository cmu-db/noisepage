
#include "self_driving/planning/mcts/tree_node.h"

#include <cmath>
#include <random>

#include "loggers/selfdriving_logger.h"
#include "self_driving/forecasting/workload_forecast.h"
#include "self_driving/planning/action/abstract_action.h"
#include "self_driving/planning/pilot.h"
#include "self_driving/planning/pilot_util.h"

#define EPSILON 1e-3

namespace noisepage::selfdriving::pilot {

TreeNode::TreeNode(common::ManagedPointer<TreeNode> parent, action_id_t current_action, double current_segment_cost,
                   double later_segments_cost)
    : is_leaf_{true},
      depth_(parent == nullptr ? 0 : parent->depth_ + 1),
      current_action_(current_action),
      ancestor_cost_(current_segment_cost + (parent == nullptr ? 0 : parent->ancestor_cost_)),
      parent_(parent),
      number_of_visits_{1} {
  if (parent != nullptr) parent->is_leaf_ = false;
  cost_ = ancestor_cost_ + later_segments_cost;
  SELFDRIVING_LOG_INFO(
      "Creating Tree Node: Depth {} Action {} Cost {} Current_Segment_Cost {} Later_Segment_Cost {} Ancestor_Cost {}",
      depth_, current_action_, cost_, current_segment_cost, later_segments_cost, ancestor_cost_);
}

common::ManagedPointer<TreeNode> TreeNode::BestSubtree() {
  NOISEPAGE_ASSERT(!is_leaf_, "Trying to return best action on a leaf node");
  // Get child of least cost
  NOISEPAGE_ASSERT(!children_.empty(), "Trying to return best action for unexpanded nodes");
  auto best_child = common::ManagedPointer(children_[0]);
  for (auto &child : children_) {
    if (child->cost_ < best_child->cost_) best_child = common::ManagedPointer(child);
    SELFDRIVING_LOG_INFO("Finding best action: Depth {} Action {} Child {} Cost {}", depth_, current_action_,
                         child->GetCurrentAction(), child->cost_);
  }
  return best_child;
}

void TreeNode::UpdateCostAndVisits(uint64_t num_expansion, double leaf_cost, double expanded_cost) {
  // compute cost as average of the leaves in its subtree weighted by the number of visits
  // Here we add the number of successors expanded in the previous rollout and also subtract 1
  // because the cost of the expanded leaf was invalidated
  auto new_num_visits = num_expansion - 1 + number_of_visits_;

  SELFDRIVING_LOG_TRACE("Depth: {} Before Update Cost: {}", depth_, cost_);
  cost_ = static_cast<double>(number_of_visits_) / new_num_visits * cost_ - leaf_cost / new_num_visits +
          static_cast<double>(num_expansion) / new_num_visits * expanded_cost;
  SELFDRIVING_LOG_TRACE("number_of_visits_ {} new_num_visits {} leaf_cost {} num_expansion {} expanded_cost {}",
                        number_of_visits_, new_num_visits, leaf_cost, num_expansion, expanded_cost);
  SELFDRIVING_LOG_TRACE("After Update Cost: {}", cost_);
  number_of_visits_ = new_num_visits;
}

common::ManagedPointer<TreeNode> TreeNode::SampleChild() {
  // compute max of children's cost
  double highest = 0;
  for (auto &child : children_) highest = std::max(child->cost_, highest);

  // sample based on cost and num of visits of children
  std::vector<double> children_weights;
  for (auto &child : children_) {
    // Adopted from recommended formula in
    // https://en.wikipedia.org/wiki/Monte_Carlo_tree_search#Exploration_and_exploitation The first additive term was
    // changed to be the inverse of a normalized value, since smaller cost is preferred
    auto child_obj = std::pow((highest + EPSILON) / (child->cost_ + EPSILON), 2) +
                     std::sqrt(2 * std::log(number_of_visits_) / child->number_of_visits_);
    children_weights.push_back(child_obj);
  }
  std::discrete_distribution<int> children_dist(children_weights.begin(), children_weights.end());
  std::random_device rd;
  std::mt19937 device(rd());
  return common::ManagedPointer(children_.at(children_dist(device)));
}

common::ManagedPointer<TreeNode> TreeNode::Selection(
    common::ManagedPointer<TreeNode> root, common::ManagedPointer<Pilot> pilot,
    const std::map<action_id_t, std::unique_ptr<AbstractAction>> &action_map,
    std::unordered_set<action_id_t> *candidate_actions, uint64_t end_segment_index) {
  common::ManagedPointer<TreeNode> curr;
  std::vector<action_id_t> actions_on_path;
  do {
    curr = root;
    actions_on_path.clear();
    while (!curr->is_leaf_) {
      curr = curr->SampleChild();
      actions_on_path.push_back(curr->current_action_);
    }
  } while (curr->depth_ > end_segment_index);

  for (auto action : actions_on_path) {
    for (auto invalid_action : action_map.at(action)->GetInvalidatedActions()) {
      candidate_actions->erase(invalid_action);
    }
    for (auto enabled_action : action_map.at(action)->GetEnabledActions()) {
      candidate_actions->insert(enabled_action);
    }
    PilotUtil::ApplyAction(pilot, action_map.at(action)->GetSQLCommand(), action_map.at(action)->GetDatabaseOid());
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
  NOISEPAGE_ASSERT(start_segment_index <= end_segment_index,
                   "start segment index should be no greater than the end segment index");

  for (const auto &action_id : candidate_actions) {
    // expand each action not yet applied
    if (!action_map.at(action_id)->IsValid() ||
        action_map.at(action_id)->GetSQLCommand() == "set compiled_query_execution = 'true';")
      continue;
    PilotUtil::ApplyAction(pilot, action_map.at(action_id)->GetSQLCommand(),
                           action_map.at(action_id)->GetDatabaseOid());

    double child_segment_cost = PilotUtil::ComputeCost(pilot, forecast, start_segment_index, start_segment_index);
    double later_segments_cost = 0;
    if (start_segment_index != end_segment_index)
      later_segments_cost = PilotUtil::ComputeCost(pilot, forecast, start_segment_index + 1, end_segment_index);

    children_.push_back(
        std::make_unique<TreeNode>(common::ManagedPointer(this), action_id, child_segment_cost, later_segments_cost));

    // apply one reverse action to undo the above
    auto rev_actions = action_map.at(action_id)->GetReverseActions();
    PilotUtil::ApplyAction(pilot, action_map.at(rev_actions[0])->GetSQLCommand(),
                           action_map.at(rev_actions[0])->GetDatabaseOid());
  }
}

void TreeNode::BackPropogate(common::ManagedPointer<Pilot> pilot,
                             const std::map<action_id_t, std::unique_ptr<AbstractAction>> &action_map,
                             bool use_min_cost) {
  auto curr = common::ManagedPointer(this);
  auto leaf_cost = cost_;
  double expanded_cost = use_min_cost ? ComputeMinCostFromChildren() : ComputeWeightedAverageCostFromChildren();

  auto num_expansion = children_.size();
  while (curr != nullptr && curr->parent_ != nullptr) {
    auto rev_action = action_map.at(curr->current_action_)->GetReverseActions()[0];
    PilotUtil::ApplyAction(pilot, action_map.at(rev_action)->GetSQLCommand(),
                           action_map.at(rev_action)->GetDatabaseOid());
    if (use_min_cost) {
      curr->cost_ = std::min(curr->cost_, expanded_cost);
    } else {
      // All ancestors of the expanded leaf need to updated with a weight increase of num_expansion, and a new weight
      curr->UpdateCostAndVisits(num_expansion, leaf_cost, expanded_cost);
    }
    curr = curr->parent_;
  }
}

}  // namespace noisepage::selfdriving::pilot
