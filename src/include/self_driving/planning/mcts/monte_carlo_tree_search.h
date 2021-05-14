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
   * Returns query string of the best action to take at the root of the current tree
   * @param simulation_number number of simulations to run
   * @param best_action_seq storing output: query string of the best first action as well as the associated database oid
   * @param memory_constraint maximum allowed memory in bytes
   */
  void BestAction(uint64_t simulation_number,
                  std::vector<std::pair<const std::string, catalog::db_oid_t>> *best_action_seq,
                  uint64_t memory_constraint);

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
