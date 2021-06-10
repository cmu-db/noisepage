#pragma once

#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "self_driving/planning/action/action_defs.h"

namespace noisepage::selfdriving {
class WorkloadForecast;

namespace pilot {
class AbstractAction;
class PlanningContext;
struct PathSolution;

/**
 * The SequenceTuning class implements the structure searching algorithm as a baseline for comparison with
 * MonteCarloTreeSearch. The algo varies from the original paper linked below by considering a set of structures at each
 * timestamp: https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/SequenceTuning_Sig06.pdf.
 */
class SequenceTuning {
 public:
  /**
   * Constructor for SequenceTuning, same as that of Monte Carlo Search Tree
   * @param planning_context pilot planning context
   * @param forecast pointer to workload forecast
   * @param end_segment_index the last segment index to be considered among the forecasted workloads
   */
  SequenceTuning(const PlanningContext &planning_context,
                 common::ManagedPointer<selfdriving::WorkloadForecast> forecast, uint64_t end_segment_index);

  /**
   * Returns sequence of actions that lead to the best sequence of configurations,
   * computed using sequence tuning with cost-based pruning & greedy merge
   * @param memory_constraint maximum allowed memory in bytes
   * @param best_actions_seq storing output: query string of the best actions as well as the associated database oid
   */
  void BestAction(uint64_t memory_constraint,
                  std::vector<std::set<std::pair<const std::string, catalog::db_oid_t>>> *best_actions_seq);

  /**
   * Computing the transition cost (measured as cumulative elapsed time of actions) from one configuration to another.
   * @param structure_map action map containing information about the indexes/structures
   * @param start_config initial config
   * @param end_config target config
   * @return cost of the transition/index creation
   */
  static double ConfigTransitionCost(
      const std::map<pilot::action_id_t, std::unique_ptr<pilot::AbstractAction>> &structure_map,
      const std::set<pilot::action_id_t> &start_config, const std::set<pilot::action_id_t> &end_config);

 private:
  /**
   * Computes the merged solution in graph formed by the configs and the union of configs
   * from two solutions using different structures
   * @param path_one first path
   * @param path_two second path
   * @param memory_constraint maximum allowed memory in bytes
   * @param merged_solution merged solution
   * @return cost of best path in merged graph
   */
  double UnionPair(const PathSolution &path_one, const PathSolution &path_two, uint64_t memory_constraint,
                   PathSolution *merged_solution);

  /**
   * Computes the global best sequence of configuration.
   * Implements the GREEDY-SEQ algo in section 6.2 of
   * https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/SequenceTuning_Sig06.pdf.
   * @param best_path_for_structure the path of configs, shortest distance (best cost) and set of configs
   * computed for each structure using cost-based pruning
   * @param memory_constraint maximum allowed memory in bytes
   * @param best_final_path best solution in final graph
   */
  void GreedySeq(const std::map<action_id_t, PathSolution> &best_path_for_structure, uint64_t memory_constraint,
                 PathSolution *best_final_path);

  /**
   * Extracts a set of actions that on application would move from start configuration to end configuration.
   * @param start_config initial configuration
   * @param end_config target configuration
   * @return actions that moves from start config to end config
   */
  static std::set<action_id_t> ExtractActionsFromConfigTransition(
      const std::map<pilot::action_id_t, std::unique_ptr<pilot::AbstractAction>> &structure_map,
      const std::set<action_id_t> &start_config, const std::set<action_id_t> &end_config);

  /**
   * Extract the best sequence of actions to apply from the optimal sequence of configuration.
   * @param best_final_path best path of configuration in the final graph computed from all candidate structures
   * @param best_actions_seq extracted best sequence of actions
   */
  void ExtractActionsFromConfigPath(
      const std::vector<std::set<action_id_t>> &best_final_config_path,
      std::vector<std::set<std::pair<const std::string, catalog::db_oid_t>>> *best_actions_seq);

  /**
   * Runs a while loop that iteratively considers the least cost solution so far, and if applicable
   * selects the best pair of sequence to merge.
   * Implements step 3 of the GREEDY-SEQ algo in the paper linked above.
   * @param global_path_set set of candidate paths
   * @param global_config_set set of configs selected by step 3 of the GREEDY-SEQ algo to be used in the final graph
   * @param memory_constraint maximum allowed memory in bytes
   */
  void MergeConfigs(std::multiset<PathSolution> *global_path_set, std::set<std::set<action_id_t>> *global_config_set,
                    uint64_t memory_constraint);

  const PlanningContext &planning_context_;
  const common::ManagedPointer<selfdriving::WorkloadForecast> forecast_;
  const uint64_t end_segment_index_;
  std::vector<double> default_segment_cost_;

  std::map<action_id_t, std::unique_ptr<AbstractAction>> structure_map_;
  std::vector<action_id_t> candidate_structures_;
};
}  // namespace pilot

}  // namespace noisepage::selfdriving
