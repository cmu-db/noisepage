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
                       uint64_t start_segment_index, uint64_t end_segment_index);

  /**
   * Returns query string of the best action to take at the root of the current tree
   * @param simulation_number number of simulations to run
   * @return query string of the best first action
   */
  const std::string BestAction(uint64_t simulation_number);

 private:
  common::ManagedPointer<Pilot> pilot_;
  common::ManagedPointer<selfdriving::WorkloadForecast> forecast_;
  const uint64_t start_segment_index_;
  const uint64_t end_segment_index_;
  std::unique_ptr<TreeNode> root_;
  std::map<action_id_t, std::unique_ptr<AbstractAction>> action_map_;
  std::vector<action_id_t> candidate_actions_;
  // i-th entry stores the db_oids related to segments from first i+1 segment index
  std::vector<std::vector<uint64_t>> db_oids_;
};
}

}  // namespace noisepage::selfdriving::pilot
