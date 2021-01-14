#pragma once

#include <map>
#include <vector>

#include "self_driving/pilot/action/abstract_action.h"
#include "self_driving/pilot/action/action_defs.h"
#include "self_driving/pilot/mcst/tree_node.h"
#include "self_driving/pilot/pilot.h"

namespace noisepage::selfdriving {
class Pilot;

namespace pilot {

/**
 * The pilot processes the query trace predictions by executing them and extracting pipeline features
 */
class MonteCarloSearchTree {
 public:
  /**
   * Constructor for the monte carlo search tree
   * @param pilot pointer to pilot
   * @param forecast pointer to workload forecast
   * @param plans vector of query plans that the search tree is responsible for
   * @param action_planning_horizon planning horizon (max depth of the tree, number of forecast segments to be considered)
   * @param start_segment_index the start segment index to be considered among the forecasted workloads
   */
  MonteCarloSearchTree(common::ManagedPointer<Pilot> pilot,
                       common::ManagedPointer<selfdriving::WorkloadForecast> forecast,
                       const std::vector<std::unique_ptr<planner::AbstractPlanNode>> &plans,
                       uint64_t action_planning_horizon, uint64_t start_segment_index);

  /**
   * Returns query string of the best action to take at the root of the current tree
   * @param simulation_number number of simulations to run
   * @return query string of the best first action
   */
  const std::string BestAction(uint64_t simulation_number);

  /**
   * Update the visits number and cost of the node and its ancestors in tree due to expansion of its children,
   * also apply reverse actions
   * @param node pointer to the tree node whose value is first updated in the backpropogation
   */
  void BackPropogate(common::ManagedPointer<TreeNode> node);

 private:
  /**
   * Recursively sample the vertex whose children will be assigned values through rollout.
   * @return
   */
  common::ManagedPointer<TreeNode> Selection(std::unordered_set<action_id_t> *candidate_actions);
  common::ManagedPointer<Pilot> pilot_;
  common::ManagedPointer<selfdriving::WorkloadForecast> forecast_;
  uint64_t start_segment_index_;
  std::unique_ptr<TreeNode> root_;
  std::map<action_id_t, std::unique_ptr<AbstractAction>> action_map_;
  std::vector<action_id_t> candidate_actions_;
  // i-th entry stores the db_oids related to segments from first i+1 segment index
  std::vector<std::vector<uint64_t>> db_oids_;
  uint64_t action_planning_horizon_;
};
}

}  // namespace noisepage::selfdriving::pilot
