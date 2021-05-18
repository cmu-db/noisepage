#pragma once

#include <map>
#include <memory>
#include <set>
#include <string>
#include <tuple>
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
class SequenceTuning {
 public:
  /**
   * Constructor for SequenceTuning, same as that of Monte Carlo Search Tree
   * @param pilot pointer to pilot
   * @param forecast pointer to workload forecast
   * @param end_segment_index the last segment index to be considered among the forecasted workloads
   */
  SequenceTuning(common::ManagedPointer<Pilot> pilot, common::ManagedPointer<selfdriving::WorkloadForecast> forecast,
                 uint64_t end_segment_index);

  /**
   * Returns sequence of actions that lead to the best sequence of configurations,
   * computed using sequence tuning with cost-based pruning & greedy merge
   * @param best_actions_seq storing output: query string of the best actions as well as the associated database oid
   * @param memory_constraint maximum allowed memory in bytes
   */
  void BestAction(std::vector<std::set<std::pair<const std::string, catalog::db_oid_t>>> *best_actions_seq,
                  uint64_t memory_constraint);

 private:
  /**
   * Computes the merged solution in graph formed by the configs and the union of configs
   * from two solutions using different structures
   * @param seq_one first solution sequence
   * @param seq_two second solution sequence
   * @param merged_solution merged solution sequence of configuration
   * @param merged_config_set set of unique configurations in merged solution
   * @return
   */
  double UnionPair(const std::vector<std::set<action_id_t>> &seq_one, const std::vector<std::set<action_id_t>> &seq_two,
                   std::vector<std::set<action_id_t>> *merged_solution,
                   std::set<std::set<action_id_t>> *merged_config_set);

  /**
   * Computes the global best sequence of configuration.
   * Implements the GREEDY-SEQ algo in section 6.2 of
   * https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/SequenceTuning_Sig06.pdf.
   * @param best_path_for_structure the path of configs, shortest distance (best cost) and set of configs
   * computed for each structure using cost-based pruning
   * @param best_actions_seq best sequence of actions to apply (a set of actions for each segment)
   */
  void GreedySeq(const std::map<action_id_t,
                                std::tuple<std::vector<std::set<action_id_t>>, std::set<std::set<action_id_t>>, double>>
                     &best_path_for_structure,
                 std::vector<std::set<action_id_t>> *best_final_config_path);

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
   * @param all_paths vector of all paths to keep track of unique paths in the
   * @param global_config_set
   */
  void MergeConfigs(
      std::set<std::pair<double, uint64_t>> *global_path_set,
      std::vector<std::tuple<std::vector<std::set<action_id_t>>, std::set<std::set<action_id_t>>, double>> *all_paths,
      std::set<std::set<action_id_t>> *global_config_set);

  const common::ManagedPointer<Pilot> pilot_;
  const common::ManagedPointer<selfdriving::WorkloadForecast> forecast_;
  const uint64_t end_segment_index_;
  std::vector<double> default_segment_cost_;

  std::map<action_id_t, std::unique_ptr<AbstractAction>> structure_map_;
  std::vector<action_id_t> candidate_structures_;
};
}  // namespace pilot

}  // namespace noisepage::selfdriving
