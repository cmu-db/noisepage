#pragma once

#include <map>
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "self_driving/planning/action/action_defs.h"
#include "self_driving/planning/seq_tuning/seq_node.h"

namespace noisepage::selfdriving {
class WorkloadForecast;

namespace pilot {
class AbstractAction;
class PlanningContext;
struct PathSolution;

/**
 * The GaphSolver class constructs a DAG in which a path from the source to destination represents a sequence of
 * configurations, one for each segment, and finds the least cost path using dynamic programming.
 */
class GraphSolver {
 public:
  /**
   * Constructor for GraphSolver. Initialize the DAG while finding the shortest via dynamic programming simultaneously.
   * @param planning_context pilot planning context
   * @param forecast pointer to workload forecast
   * @param end_segment_index the last segment index to be considered among the forecasted workloads
   * @param structure_map map from each action id to a structure/create index action
   * @param default_segment_cost cost of each segment when no structure is present
   * @param candidate_configurations_by_segment set of candidate configurations per segment
   * @param memory_constraint maximum allowed memory in bytes
   */
  GraphSolver(const PlanningContext &planning_context, common::ManagedPointer<selfdriving::WorkloadForecast> forecast,
              uint64_t end_segment_index, const std::map<action_id_t, std::unique_ptr<AbstractAction>> &structure_map,
              const std::vector<double> &default_segment_cost,
              const std::vector<std::set<std::set<action_id_t>>> &candidate_configurations_by_segment,
              uint64_t memory_constraint);

  /**
   * Find a shortest path in the current graph.
   * Breaking tie arbitrarily.
   * @param shortest_path a struct to be populated with information about the shortest path in DAG
   * @return length of shortest path in the DAG
   */
  double RecoverShortestPath(PathSolution *shortest_path);

 private:
  /**
   * Is a config valid by having all structures in it valid.
   * @param planning_context pilot planning context
   * @param structure_map map from each action id to a structure/create index action
   * @param config_set set of structures constituting the current config
   * @return if all structure in the config are valid
   */
  static bool IsValidConfig(const PlanningContext &planning_context,
                            const std::map<action_id_t, std::unique_ptr<AbstractAction>> &structure_map,
                            const std::set<action_id_t> &config_set);

  /**
   * Computes the cost of a segment with a certain configuration.
   * @param planning_context pilot planning context
   * @param forecast pointer to workload forecast
   * @param structure_map map from each action id to a structure/create index action
   * @param config_set set of structures constituting the current config
   * @param segment_index index of current segment
   * @return cost of executing queries in current segment with the set of structures in current config
   */
  static double ComputeConfigCost(const PlanningContext &planning_context,
                                  common::ManagedPointer<selfdriving::WorkloadForecast> forecast,
                                  const std::map<action_id_t, std::unique_ptr<AbstractAction>> &structure_map,
                                  const std::set<action_id_t> &config_set, uint64_t segment_index);

  static constexpr double MEMORY_CONSUMPTION_VIOLATION_COST = 1e10;

  std::vector<std::vector<std::unique_ptr<SeqNode>>> nodes_by_segment_index_;
  std::vector<std::unique_ptr<SeqNode>> source_level_;
  std::unique_ptr<SeqNode> dest_node_;
};
}  // namespace pilot

}  // namespace noisepage::selfdriving
