#pragma once

#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "self_driving/planning/action/abstract_action.h"
#include "self_driving/planning/action/action_defs.h"
#include "self_driving/planning/pilot.h"
#include "self_driving/planning/seq_tuning/seq_node.h"

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
   * Constructor for GraphSolver. Initialize the DAG while finding the shortest via dynamic programming simultaneously.
   * @param pilot pointer to pilot
   * @param forecast pointer to workload forecast
   * @param end_segment_index the last segment index to be considered among the forecasted workloads
   * @param structure_map map from each action id to a structure/create index action
   * @param default_segment_cost cost of each segment when no structure is present
   * @param candidate_configurations_by_segment
   * @param memory_constraint
   */
  GraphSolver(common::ManagedPointer<Pilot> pilot, common::ManagedPointer<selfdriving::WorkloadForecast> forecast,
              uint64_t end_segment_index, const std::map<action_id_t, std::unique_ptr<AbstractAction>> &structure_map,
              const std::vector<double> &default_segment_cost,
              const std::vector<std::vector<std::set<action_id_t>>> &candidate_configurations_by_segment,
              uint64_t memory_constraint);

  /**
   *
   * @param pilot
   * @param structure_map
   * @param config_set
   * @return
   */
  bool IsValidConfig(common::ManagedPointer<Pilot> pilot,
                     const std::map<action_id_t, std::unique_ptr<AbstractAction>> &structure_map,
                     const std::set<action_id_t> &config_set);

  /**
   *
   * @param pilot
   * @param forecast
   * @param structure_map
   * @param config_set
   * @param segment_index
   * @return
   */
  double ComputeConfigCost(common::ManagedPointer<Pilot> pilot,
                           common::ManagedPointer<selfdriving::WorkloadForecast> forecast,
                           const std::map<action_id_t, std::unique_ptr<AbstractAction>> &structure_map,
                           const std::set<action_id_t> &config_set, uint64_t segment_index);

  /**
   * Find a shortest path in the current graph to populate the best config sequence.
   * Also collect the set of unique configurations on the best config sequence.
   * Breaking tie arbitrarily.
   * @param best_config_path
   * @param best_config_set
   * @return
   */
  double RecoverShortestPath(std::vector<std::set<action_id_t>> *best_config_path,
                             std::set<std::set<action_id_t>> *best_config_set);

 private:
  static constexpr double MEMORY_CONSUMPTION_VIOLATION_COST = 1e10;

  std::vector<std::vector<std::unique_ptr<SeqNode>>> nodes_by_segment_index_;
  std::vector<std::unique_ptr<SeqNode>> source_level_;
  std::unique_ptr<SeqNode> dest_node_;
};
}  // namespace pilot

}  // namespace noisepage::selfdriving
