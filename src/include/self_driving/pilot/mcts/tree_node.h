#pragma once

#include <map>
#include <memory>
#include <unordered_set>
#include <vector>

#include "common/managed_pointer.h"
#include "self_driving/pilot/action/action_defs.h"

#define EPSILON 1e-3
#define NULL_ACTION INT32_MAX

namespace noisepage::selfdriving {
class Pilot;
class WorkloadForecast;

namespace pilot {
class AbstractAction;

/**
 * The pilot processes the query trace predictions by executing them and extracting pipeline features
 */
class TreeNode {
 public:
  /**
   * Constructor for tree node
   * @param parent pointer to parent
   * @param current_action action that leads its parent to the current node, root has NULL action
   * @param current_segment_cost cost of executing current segment with actions applied on path from root to current
   * node
   * @param later_segments_cost cost of later segments when actions applied on path from root to current node
   */
  TreeNode(common::ManagedPointer<TreeNode> parent, action_id_t current_action, uint64_t current_segment_cost,
           uint64_t later_segments_cost);

  /**
   * @return action id at node with least cost
   */
  common::ManagedPointer<TreeNode> BestSubtree();

  /**
   * Recursively sample the vertex whose children will be assigned values through rollout.
   * @param root pointer to root of the search tree
   * @param pilot pointer to pilot
   * @param action_map action map of the search tree
   * @param candidate_actions candidate actions that can be applied at curent node
   * @param end_segment_index last segment index to be considered in forecast (needed so that when sampled leaf is
   * beyond this index, we repeat the selection process)
   */
  static common::ManagedPointer<TreeNode> Selection(
      common::ManagedPointer<TreeNode> root, common::ManagedPointer<Pilot> pilot,
      const std::map<action_id_t, std::unique_ptr<AbstractAction>> &action_map,
      std::unordered_set<action_id_t> *candidate_actions, uint64_t end_segment_index);

  /**
   * Expand each child of current node and update its cost and num of visits accordingly
   * @param pilot pointer to pilot
   * @param forecast pointer to forecasted workload
   * @param tree_start_segment_index start_segment_index of the search tree
   * @param tree_end_segment_index end_segment_index of the search tree
   * @param action_map action map of the search tree
   * @param candidate_actions candidate actions of the search tree
   */
  void ChildrenRollout(common::ManagedPointer<Pilot> pilot, common::ManagedPointer<WorkloadForecast> forecast,
                       uint64_t tree_start_segment_index, uint64_t tree_end_segment_index,
                       const std::map<action_id_t, std::unique_ptr<AbstractAction>> &action_map,
                       const std::unordered_set<action_id_t> &candidate_actions);

  /**
   * Update the visits number and cost of the node and its ancestors in tree due to expansion of its children,
   * also apply reverse actions
   * @param pilot pointer to pilot
   * @param action_map action map of the search tree
   */
  void BackPropogate(common::ManagedPointer<Pilot> pilot,
                     const std::map<action_id_t, std::unique_ptr<AbstractAction>> &action_map);

  /**
   * Return if current node is a leaf
   * @return is_leaf_
   */
  bool IsLeaf() { return is_leaf_; }

  /**
   * Get action taken to get to this node from its parent
   * @return current action
   */
  action_id_t GetCurrentAction() { return current_action_; }

 private:
  /**
   * Sample child based on cost and number of visits
   * @return pointer to sampled child
   */
  common::ManagedPointer<TreeNode> SampleChild();

  /**
   * Compute cost as average of children weighted by num of visits of each one
   * (usually only used on the leaf being expanded in a simulation round, for nonleaf see UpdateCostAndVisits)
   * @return recomputed cost of current node
   */
  uint64_t ComputeCostFromChildren() {
    uint64_t child_sum = 0, total_visits = 0;
    for (auto &child : children_) {
      child_sum += child->cost_ * child->number_of_visits_;
      total_visits += child->number_of_visits_;
    }
    NOISEPAGE_ASSERT(total_visits > 0, "num visit of the subtree rooted at a node cannot be zero");
    return child_sum / (total_visits + EPSILON);
  }

  /**
   * Update number of visits to the current node aka number of tree traversals to a leaf
   * containing the path to current node, and the cost of the node; based on the expansion of a successor as a leaf
   * @param num_expansion number of children of the expanded leaf
   * @param leaf_cost previous cost of the leaf
   * @param expanded_cost new cost of the leaf after expansion
   */
  void UpdateCostAndVisits(uint64_t num_expansion, uint64_t leaf_cost, uint64_t expanded_cost);

  bool is_leaf_;
  const uint64_t depth_;  // number of edges in path from root
  const action_id_t current_action_;
  const uint64_t ancestor_cost_;  // cost of executing segments with actions applied on path from root to current node
  const common::ManagedPointer<TreeNode> parent_;

  uint64_t number_of_visits_;  // number of leaf in subtree rooted at node
  std::vector<std::unique_ptr<TreeNode>> children_;
  uint64_t cost_{UINT64_MAX};
};
}  // namespace pilot

}  // namespace noisepage::selfdriving
