#pragma once

#include <map>
#include <memory>
#include <string>
#include <set>
#include <utility>
#include <vector>

#include "self_driving/planning/action/abstract_action.h"
#include "self_driving/planning/action/action_defs.h"
#include "self_driving/planning/seq_tuning/seq_node.h"
#include "self_driving/planning/pilot.h"

#define EPSILON 1e-3

namespace noisepage::selfdriving {
class Pilot;
class WorkloadForecast;

namespace pilot {
class AbstractAction;

/**
 * The pilot processes the query trace predictions by executing them and extracting pipeline features
 */
class GraphSolver {

 public:

  /**
   *
   * @param pilot
   * @param forecast
   * @param end_segment_index
   * @param action_map
   * @param default_segment_cost
   * @param candidate_configurations
   * @param memory_constraint
   */
  GraphSolver(common::ManagedPointer<Pilot> pilot,
              common::ManagedPointer<selfdriving::WorkloadForecast> forecast, uint64_t end_segment_index,
              const std::map<action_id_t, std::unique_ptr<AbstractAction>> &action_map,
              std::vector<double> default_segment_cost,
              std::vector<std::vector<std::set<action_id_t>>> candidate_configurations_by_segment,
              uint64_t memory_constraint);

  /**
   *
   * @param pilot
   * @param action_map
   * @param config_set
   * @param actions_applied
   * @param memory_constraint
   * @return
   */
  bool IsValidConfig(common::ManagedPointer<Pilot> pilot,
                     const std::map<action_id_t, std::unique_ptr<AbstractAction>> &action_map,
                     const std::set<action_id_t> &config_set,
                     std::set<action_id_t> *actions_applied, uint64_t memory_constraint);

  /**
   * Find a shortest path in the current graph to populate the best action sequence.
   * Breaking tie arbitrarily.
   * @param best_config_path
   * @return
   */
  double RecoverShortestPath(std::vector<std::set<action_id_t>> *best_config_path,
                             std::set<std::set<action_id_t>> *best_config_set_for_curr_action);

 private:
  std::vector<std::vector<std::unique_ptr<SeqNode>>> nodes_by_segment_index_;
  std::vector<std::unique_ptr<SeqNode>> source_level_;
  std::unique_ptr<SeqNode> dest_node_;
};
}  // namespace pilot

}  // namespace noisepage::selfdriving
