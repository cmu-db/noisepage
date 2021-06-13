#include "self_driving/planning/mcts/tree_node.h"

#include <cmath>
#include <random>

#include "common/strong_typedef_body.h"
#include "loggers/selfdriving_logger.h"
#include "self_driving/forecasting/workload_forecast.h"
#include "self_driving/planning/action/abstract_action.h"
#include "self_driving/planning/action/create_index_action.h"
#include "self_driving/planning/pilot.h"
#include "self_driving/planning/pilot_util.h"
#include "self_driving/planning/planning_context.h"

#define EPSILON 1e-3

namespace noisepage::selfdriving::pilot {

STRONG_TYPEDEF_BODY(tree_node_id_t, uint64_t);

tree_node_id_t TreeNode::tree_node_identifier = tree_node_id_t(1);

TreeNode::TreeNode(common::ManagedPointer<TreeNode> parent, action_id_t current_action,
                   uint64_t action_start_segment_index, double current_segment_cost, double later_segments_cost,
                   uint64_t memory, ActionState action_state)
    : tree_node_id_(TreeNode::tree_node_identifier++),
      is_leaf_{true},
      depth_(parent == nullptr ? 0 : parent->depth_ + 1),
      action_start_segment_index_(action_start_segment_index),
      action_plan_end_index_(action_start_segment_index_),
      current_action_(current_action),
      ancestor_cost_(current_segment_cost + (parent == nullptr ? 0 : parent->ancestor_cost_)),
      parent_(parent),
      number_of_visits_{1},
      memory_(memory),
      action_state_(std::move(action_state)) {
  if (parent != nullptr) parent->is_leaf_ = false;
  cost_ = ancestor_cost_ + later_segments_cost;
  SELFDRIVING_LOG_INFO(
      "Creating Tree Node: Depth {} Action Start Segment Index {} Action {} Cost {} Current_Segment_Cost {} "
      "Later_Segment_Cost {} Ancestor_Cost {}",
      depth_, action_start_segment_index_, current_action_, cost_, current_segment_cost, later_segments_cost,
      ancestor_cost_);

  // TODO(lin): Add the memory information to the action recording table
  (void)memory_;
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

std::vector<common::ManagedPointer<TreeNode>> TreeNode::BestSubtreeOrdering() {
  NOISEPAGE_ASSERT(!is_leaf_, "Trying to return best action on a leaf node");
  // Get child of least cost
  NOISEPAGE_ASSERT(!children_.empty(), "Trying to return best action for unexpanded nodes");

  std::vector<common::ManagedPointer<TreeNode>> results;
  results.reserve(children_.size());
  for (auto &child : children_) {
    results.emplace_back(child);
  }

  struct {
    bool operator()(common::ManagedPointer<TreeNode> a, common::ManagedPointer<TreeNode> b) {
      return a->cost_ < b->cost_;
    }
  } cmp;
  std::sort(results.begin(), results.end(), cmp);
  return results;
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
    common::ManagedPointer<TreeNode> root, const PlanningContext &planning_context,
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
  } while (curr->action_start_segment_index_ > end_segment_index);

  for (auto action : actions_on_path) {
    for (auto invalid_action : action_map.at(action)->GetInvalidatedActions()) {
      candidate_actions->erase(invalid_action);
    }
    for (auto enabled_action : action_map.at(action)->GetEnabledActions()) {
      candidate_actions->insert(enabled_action);
    }
    PilotUtil::ApplyAction(planning_context, action_map.at(action)->GetSQLCommand(),
                           action_map.at(action)->GetDatabaseOid(), Pilot::WHAT_IF);
  }
  return curr;
}

void TreeNode::ChildrenRollout(PlanningContext *planning_context,
                               common::ManagedPointer<selfdriving::WorkloadForecast> forecast, uint64_t action_horizon,
                               uint64_t tree_end_segment_index,
                               const std::map<action_id_t, std::unique_ptr<AbstractAction>> &action_map,
                               const std::unordered_set<action_id_t> &candidate_actions,
                               std::unordered_map<ActionState, double, ActionStateHasher> *action_state_cost_map,
                               uint64_t memory_constraint) {
  action_plan_end_index_ = std::min(action_start_segment_index_ + action_horizon - 1, tree_end_segment_index);

  SELFDRIVING_LOG_DEBUG("action_start_segment_index: {} action_plan_end_index: {} tree_end_segment_index: {}",
                        action_start_segment_index_, action_plan_end_index_, tree_end_segment_index);
  NOISEPAGE_ASSERT(action_start_segment_index_ <= tree_end_segment_index,
                   "action plan end segment index should be no greater than tree end segment index");

  auto new_action_state = action_state_;

  for (const auto &action_id : candidate_actions) {
    // expand each action not yet applied
    auto const &action_ptr = action_map.at(action_id);
    if (!action_ptr->IsValid() || action_ptr->GetSQLCommand() == "set compiled_query_execution = 'true';") continue;

    // Update the action state assuming this action is applied
    action_ptr->ModifyActionState(&new_action_state);

    // Compute memory consumption
    bool satisfy_memory_constraint = true;
    // We may apply actions to reduce memory consumption in future, so we only need to evaluate the memory constraint
    // up to action_plan_end_index_
    for (auto segment_index = action_start_segment_index_; segment_index <= action_plan_end_index_; segment_index++) {
      size_t memory = PilotUtil::CalculateMemoryConsumption(planning_context->GetMemoryInfo(), new_action_state,
                                                            segment_index, action_map);
      if (memory > memory_constraint) satisfy_memory_constraint = false;
    }
    // For bookkeeping purpose
    size_t action_plan_end_memory_consumption = PilotUtil::CalculateMemoryConsumption(
        planning_context->GetMemoryInfo(), new_action_state, action_plan_end_index_, action_map);

    // Initialize to large enough value when the memory constraint is not satisfied
    double child_segment_cost = MEMORY_CONSUMPTION_VIOLATION_COST;
    double later_segments_cost = MEMORY_CONSUMPTION_VIOLATION_COST;
    if (satisfy_memory_constraint) {
      PilotUtil::ApplyAction(*planning_context, action_ptr->GetSQLCommand(), action_ptr->GetDatabaseOid(),
                             Pilot::WHAT_IF);
      auto reverse_actions = action_ptr->GetReverseActions();
      auto reverse_action = reverse_actions[0];

      new_action_state.SetIntervals(action_start_segment_index_, action_plan_end_index_);
      if (action_ptr->GetActionType() != ActionType::CREATE_INDEX) {
        // Action immediately takes effect. Can use the cost cache
        if (action_state_cost_map->find(new_action_state) != action_state_cost_map->end()) {
          child_segment_cost = action_state_cost_map->at(new_action_state);
          SELFDRIVING_LOG_TRACE("Get child cost from map with action {} start interval {} end interval {}: {}",
                                action_ptr->GetActionID().UnderlyingValue(), action_start_segment_index_,
                                action_plan_end_index_, child_segment_cost);
        } else {
          // Compute cost and add to the cache
          child_segment_cost = PilotUtil::ComputeCost(planning_context, forecast, action_start_segment_index_,
                                                      action_plan_end_index_, std::nullopt, std::nullopt);
          action_state_cost_map->emplace(std::make_pair(new_action_state, child_segment_cost));
        }
      } else {
        // To pass calculate to get the action duration considering the interference model. Cannot use the cost cache

        // First reverse the action before computing the cost
        PilotUtil::ApplyAction(*planning_context, action_map.at(reverse_action)->GetSQLCommand(),
                               action_map.at(reverse_action)->GetDatabaseOid(), Pilot::WHAT_IF);
        child_segment_cost =
            ComputeCostWithAction(planning_context, forecast, tree_end_segment_index, action_ptr.get());

        // Apply the action back again
        PilotUtil::ApplyAction(*planning_context, action_ptr->GetSQLCommand(), action_ptr->GetDatabaseOid(),
                               Pilot::WHAT_IF);
        // Re-calculate the memory consumption in case action_plan_end_index_ is modified
        action_plan_end_memory_consumption = PilotUtil::CalculateMemoryConsumption(
            planning_context->GetMemoryInfo(), new_action_state, action_plan_end_index_, action_map);
        if (action_plan_end_memory_consumption > memory_constraint) satisfy_memory_constraint = false;
      }

      if (satisfy_memory_constraint) {
        new_action_state.SetIntervals(action_plan_end_index_ + 1, tree_end_segment_index);
        if (action_state_cost_map->find(new_action_state) != action_state_cost_map->end()) {
          later_segments_cost = action_state_cost_map->at(new_action_state);
          SELFDRIVING_LOG_TRACE("Get later cost from map with action {} start interval {} end interval {}: {}",
                                action_ptr->GetActionID().UnderlyingValue(), action_plan_end_index_ + 1,
                                tree_end_segment_index, later_segments_cost);
        } else {
          // Compute cost and add to the cache
          if (action_plan_end_index_ == tree_end_segment_index)
            later_segments_cost = 0;
          else
            later_segments_cost = PilotUtil::ComputeCost(planning_context, forecast, action_plan_end_index_ + 1,
                                                         tree_end_segment_index, std::nullopt, std::nullopt);
          action_state_cost_map->emplace(std::make_pair(new_action_state, later_segments_cost));
        }
      }

      // apply one reverse action to undo the above
      PilotUtil::ApplyAction(*planning_context, action_map.at(reverse_action)->GetSQLCommand(),
                             action_map.at(reverse_action)->GetDatabaseOid(), Pilot::WHAT_IF);
    }

    // Add new child with proper action state
    new_action_state.SetIntervals(action_plan_end_index_ + 1, tree_end_segment_index);
    children_.push_back(std::make_unique<TreeNode>(common::ManagedPointer(this), action_id, action_plan_end_index_ + 1,
                                                   child_segment_cost, later_segments_cost,
                                                   action_plan_end_memory_consumption, new_action_state));

    // Reverse the action state
    action_map.at(action_ptr->GetReverseActions()[0])->ModifyActionState(&new_action_state);

    // Restore action_plan_end_index_ in case modified by ComputeCostWithAction()
    action_plan_end_index_ = std::min(action_start_segment_index_ + action_horizon - 1, tree_end_segment_index);
  }
}

double TreeNode::ComputeCostWithAction(PlanningContext *planning_context,
                                       common::ManagedPointer<WorkloadForecast> forecast,
                                       uint64_t tree_end_segment_index, AbstractAction *action) {
  printf("********************\n");
  // How many segments does it take for this action to finish
  uint64_t action_segments = 0;
  double estimated_elapsed = action->GetEstimatedElapsedUs();
  printf("Original estimated action elapsed time: %f\n", estimated_elapsed);
  if (estimated_elapsed > 1e-6)
    action_segments = static_cast<uint64_t>(estimated_elapsed) / forecast->GetForecastInterval() + 1;
  // Cannot exceed tree_end_segment_index
  action_segments = std::min(action_segments, tree_end_segment_index - action_start_segment_index_ + 1);
  uint64_t action_end_segment = action_start_segment_index_ + action_segments - 1;
  if (action_end_segment > action_plan_end_index_) action_plan_end_index_ = action_end_segment;
  // Save the initial value
  uint64_t initial_action_segments = action_segments;
  double cost = PilotUtil::ComputeCost(planning_context, forecast, action_start_segment_index_, action_plan_end_index_,
                                       action, &action_segments);
  printf("Action takes %lu segments with cost: %f\n", action_segments, cost);
  action_segments = std::min(action_segments, tree_end_segment_index - action_start_segment_index_ + 1);
  action_segments = 5;
  // Recalculate cost if the action takes longer than initial_action_segments due to the interference
  if (action_segments > initial_action_segments) {
    // Update the action_plan_end_index_ again
    action_end_segment = action_start_segment_index_ + action_segments - 1;
    if (action_end_segment > action_plan_end_index_) action_plan_end_index_ = action_end_segment;
    cost = PilotUtil::ComputeCost(planning_context, forecast, action_start_segment_index_, action_plan_end_index_,
                                  action, &action_segments);
    printf("Action takes %lu final segments with new cost: %f\n", action_segments, cost);
  }
  printf("********************\n");
  return cost;
}

void TreeNode::BackPropogate(const PlanningContext &planning_context,
                             const std::map<action_id_t, std::unique_ptr<AbstractAction>> &action_map,
                             bool use_min_cost) {
  auto curr = common::ManagedPointer(this);
  auto leaf_cost = cost_;
  double expanded_cost = use_min_cost ? ComputeMinCostFromChildren() : ComputeWeightedAverageCostFromChildren();

  auto num_expansion = children_.size();
  while (curr != nullptr && curr->parent_ != nullptr) {
    auto rev_action = action_map.at(curr->current_action_)->GetReverseActions()[0];
    PilotUtil::ApplyAction(planning_context, action_map.at(rev_action)->GetSQLCommand(),
                           action_map.at(rev_action)->GetDatabaseOid(), Pilot::WHAT_IF);
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
