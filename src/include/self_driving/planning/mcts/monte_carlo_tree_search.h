#pragma once

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "self_driving/planning/action/abstract_action.h"
#include "self_driving/planning/action/action_defs.h"
#include "self_driving/planning/mcts/tree_node.h"
#include "self_driving/planning/pilot.h"

namespace noisepage::selfdriving {
class Pilot;

namespace pilot {

/**
 * Used to represent information that encapsulates a given tree
 * node's information and information about the action to be applied.
 */
class ActionTreeNode {
 public:
  /**
   * Constructor
   * @param parent_node_id Parent Node Identifier
   * @param tree_node_id Tree Node Identifier
   * @param action_id Action Identifier
   * @param cost Cost to apply
   * @param db_oid Database OID that action acts on
   * @param action_text SQL string associated with action
   * @param action_start_segment_index start of segment index that this node will influence
   * @param action_plan_end_index end of segment index for planning
   */
  ActionTreeNode(tree_node_id_t parent_node_id, tree_node_id_t tree_node_id, action_id_t action_id, double cost,
                 catalog::db_oid_t db_oid, std::string action_text, uint64_t action_start_segment_index,
                 uint64_t action_plan_end_index)
      : parent_node_id_(parent_node_id),
        tree_node_id_(tree_node_id),
        action_id_(action_id),
        cost_(cost),
        db_oid_(db_oid),
        action_text_(std::move(action_text)),
        action_start_segment_index_(action_start_segment_index),
        action_plan_end_index_(action_plan_end_index) {}

  /** @return parent node id */
  tree_node_id_t GetParentNodeId() const { return parent_node_id_; }

  /** @return tree node id */
  tree_node_id_t GetTreeNodeId() const { return tree_node_id_; }

  /** @return action ID */
  action_id_t GetActionId() const { return action_id_; }

  /** @return cost */
  double GetCost() const { return cost_; }

  /** @return database OID that action acts upon */
  catalog::db_oid_t GetDbOid() const { return db_oid_; }

  /** @return action text */
  const std::string &GetActionText() const { return action_text_; }

  /** @return action start segment index */
  uint64_t GetActionStartSegmentIndex() const { return action_start_segment_index_; }

  /** @return action plan end index */
  uint64_t GetActionPlanEndIndex() const { return action_plan_end_index_; }

 private:
  tree_node_id_t parent_node_id_;
  tree_node_id_t tree_node_id_;
  action_id_t action_id_;
  double cost_;
  catalog::db_oid_t db_oid_;
  std::string action_text_;
  uint64_t action_start_segment_index_;
  uint64_t action_plan_end_index_;
};

/**
 * The pilot processes the query trace predictions by executing them and extracting pipeline features
 */
class MonteCarloTreeSearch {
 public:
  /**
   * Constructor for the monte carlo search tree
   * @param pilot pointer to pilot
   * @param forecast pointer to workload forecast
   * @param end_segment_index the last segment index to be considered among the forecasted workloads
   * @param use_min_cost whether to use the minimum cost of all leaves as the cost for internal nodes
   */
  MonteCarloTreeSearch(common::ManagedPointer<Pilot> pilot,
                       common::ManagedPointer<selfdriving::WorkloadForecast> forecast, uint64_t end_segment_index,
                       bool use_min_cost = true);

  /**
   * Runs the monte carlo tree search simulations
   * @param simulation_number number of simulations to run
   * @param memory_constraint maximum allowed memory in bytes
   */
  void RunSimulation(uint64_t simulation_number, uint64_t memory_constraint);

  /**
   * Calculates the best action sequence from the root.
   * Then for each node on the path, topk - 1 of its optimal siblings
   * are also added to its vector in best_action_seq
   *
   * @param best_action_seq storing output of ActionTreeNode
   * @param topk number of nodes per level
   */
  void BestAction(std::vector<std::vector<ActionTreeNode>> *best_action_seq, size_t topk);

 private:
  const common::ManagedPointer<Pilot> pilot_;
  const common::ManagedPointer<selfdriving::WorkloadForecast> forecast_;
  const uint64_t end_segment_index_;
  std::unique_ptr<TreeNode> root_;
  std::map<action_id_t, std::unique_ptr<AbstractAction>> action_map_;
  std::vector<action_id_t> candidate_actions_;
  bool use_min_cost_;  // Use the minimum cost of all leaves (instead of the average) as the cost for internal nodes
  std::vector<uint64_t> levels_to_plan_ = {1, 2, 2, 3, 3, 3, 4, 4, 4};
};
}  // namespace pilot

}  // namespace noisepage::selfdriving
