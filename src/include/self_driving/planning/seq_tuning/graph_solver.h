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
   *
   * @param pilot
   * @param forecast
   * @param end_segment_index
   * @param structure_map
   * @param default_segment_cost
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
   * @param actions_applied
   * @param memory_constraint
   * @return
   */
  bool IsValidConfig(common::ManagedPointer<Pilot> pilot,
                     const std::map<action_id_t, std::unique_ptr<AbstractAction>> &structure_map,
                     const std::set<action_id_t> &config_set);

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
