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
  MonteCarloSearchTree(common::ManagedPointer<Pilot> pilot,
                       common::ManagedPointer<selfdriving::WorkloadForecast> forecast,
                       const std::vector<std::unique_ptr<planner::AbstractPlanNode>> &plans,
                       uint64_t action_planning_horizon,
                       uint64_t simulation_number, uint64_t start_segment_index);
  const std::string BestAction(std::map<pilot::action_id_t, std::unique_ptr<pilot::AbstractAction>> *best_action_map,
                               std::vector<pilot::action_id_t> *best_action_seq);
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
  std::vector<std::vector<uint64_t>> db_oids_;
  uint64_t action_planning_horizon_;
  uint64_t simulation_number_;
};
}

}  // namespace noisepage::selfdriving::pilot
