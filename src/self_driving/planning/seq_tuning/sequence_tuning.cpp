#include "self_driving/planning/seq_tuning/sequence_tuning.h"

#include "common/managed_pointer.h"
#include "loggers/selfdriving_logger.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "self_driving/planning/action/create_index_action.h"
#include "self_driving/planning/action/generators/change_knob_action_generator.h"
#include "self_driving/planning/action/generators/index_action_generator.h"
#include "self_driving/planning/pilot.h"
#include "self_driving/planning/pilot_util.h"
#include "self_driving/planning/seq_tuning/graph_solver.h"
#include "self_driving/planning/seq_tuning/path_solution.h"

namespace noisepage::selfdriving::pilot {

SequenceTuning::SequenceTuning(const PlanningContext &planning_context,
                               common::ManagedPointer<selfdriving::WorkloadForecast> forecast,
                               uint64_t end_segment_index)
    : planning_context_(planning_context), forecast_(forecast), end_segment_index_(end_segment_index) {
  std::vector<std::unique_ptr<planner::AbstractPlanNode>> plans;
  // vector of query plans that the search tree is responsible for
  PilotUtil::GetQueryPlans(planning_context_, common::ManagedPointer(forecast_), end_segment_index, &plans);

  std::vector<action_id_t> candidate_actions;
  // populate structure_map_, candidate_structures_
  IndexActionGenerator().GenerateActions(plans, planning_context_.GetSettingsManager(), &structure_map_,
                                         &candidate_actions);

  for (auto &action : candidate_actions)
    if (structure_map_.at(action)->GetActionType() == ActionType::CREATE_INDEX) {
      candidate_structures_.emplace_back(action);

      auto create_action = reinterpret_cast<CreateIndexAction *>(structure_map_.at(action).get());
      auto drop_action = reinterpret_cast<DropIndexAction *>(
          structure_map_.at(structure_map_.at(action)->GetReverseActions().at(0)).get());
      PilotUtil::EstimateCreateIndexAction(planning_context_, create_action, drop_action);

      SELFDRIVING_LOG_DEBUG("Candidate structure: ID {} Command {}", action,
                            structure_map_.at(action)->GetSQLCommand());
    }

  for (const auto &it UNUSED_ATTRIBUTE : structure_map_) {
    SELFDRIVING_LOG_DEBUG("Generated action: ID {} Command {}", it.first, it.second->GetSQLCommand());
  }

  std::vector<double> default_segment_cost;
  // first compute the cost of each segment when no action is applied
  for (uint64_t segment_index = 0; segment_index <= end_segment_index_; segment_index++) {
    default_segment_cost.emplace_back(
        PilotUtil::ComputeCost(planning_context_, forecast_, segment_index, segment_index));
  }
  default_segment_cost_ = default_segment_cost;
}

void SequenceTuning::BestAction(
    uint64_t memory_constraint,
    std::vector<std::set<std::pair<const std::string, catalog::db_oid_t>>> *best_actions_seq) {
  // cost-based pruning for each individual action
  std::map<action_id_t, PathSolution> best_path_for_structure;

  for (auto structure_id : candidate_structures_) {
    PathSolution best_solution;

    std::set<std::set<action_id_t>> singleton_action = {{structure_id}};
    std::vector<std::set<std::set<action_id_t>>> singleton_action_repeated(end_segment_index_ + 1, singleton_action);

    double best_path_cost UNUSED_ATTRIBUTE =
        GraphSolver(planning_context_, forecast_, end_segment_index_, structure_map_, default_segment_cost_,
                    singleton_action_repeated, memory_constraint)
            .RecoverShortestPath(&best_solution);

    SELFDRIVING_LOG_INFO("[cost-based pruning] for structure \"{}\" finds best path distance {} with {} configs",
                         structure_map_.at(structure_id)->GetSQLCommand(), best_path_cost,
                         best_solution.unique_config_on_path_.size());

    best_path_for_structure.emplace(structure_id, std::move(best_solution));
  }

  // since we only have one sequence (no split step), directly apply greedy-sequence algo to merge individual paths to
  // get the final solution, (no merge step at the end)
  PathSolution best_final_path;
  GreedySeq(best_path_for_structure, memory_constraint, &best_final_path);
  ExtractActionsFromConfigPath(best_final_path.config_on_path_, best_actions_seq);
}

double SequenceTuning::UnionPair(const PathSolution &path_one, const PathSolution &path_two, uint64_t memory_constraint,
                                 PathSolution *merged_solution) {
  std::vector<std::set<std::set<action_id_t>>> candidate_structures_by_segment;

  auto const &seq_one = path_one.config_on_path_;
  auto const &seq_two = path_two.config_on_path_;

  NOISEPAGE_ASSERT(seq_one.size() == seq_two.size() && seq_one.size() == end_segment_index_ + 2,
                   "UnionPair requires two sequences both of length end_segment_index_ + 2");

  // merge the configurations for each segment, here we skip index 0 that corresponds to the dummy source
  for (uint64_t structure_idx = 1; structure_idx < seq_one.size(); structure_idx++) {
    std::set<std::set<action_id_t>> curr_level;
    curr_level.insert(seq_one.at(structure_idx));
    curr_level.insert(seq_two.at(structure_idx));

    std::set<action_id_t> unioned_config;
    unioned_config.insert(seq_one.at(structure_idx).begin(), seq_one.at(structure_idx).end());
    unioned_config.insert(seq_two.at(structure_idx).begin(), seq_two.at(structure_idx).end());
    curr_level.insert(unioned_config);
    candidate_structures_by_segment.push_back(std::move(curr_level));
  }

  double best_unioned_dist = GraphSolver(planning_context_, forecast_, end_segment_index_, structure_map_,
                                         default_segment_cost_, candidate_structures_by_segment, memory_constraint)
                                 .RecoverShortestPath(merged_solution);
  return best_unioned_dist;
}

void SequenceTuning::GreedySeq(const std::map<action_id_t, PathSolution> &best_path_for_structure,
                               uint64_t memory_constraint, PathSolution *best_final_path) {
  std::set<std::set<action_id_t>> global_config_set;
  // set of solutions P, used to find the candidate configurations in GREEDY-SEQ
  std::multiset<PathSolution> global_path_set;

  // initialize global_path_set as the set of all paths found in cost-based pruning
  // initialize global_config_set to contain empty or singleton configs for each potential structure
  for (auto const &path_it : best_path_for_structure) {
    auto const &config_set = path_it.second.unique_config_on_path_;
    global_config_set.insert(config_set.begin(), config_set.end());
    global_path_set.emplace(path_it.second);
  }

  // execute the while loop in step 3 of the paper to find the set of configurations in final graph
  MergeConfigs(&global_path_set, &global_config_set, memory_constraint);
  std::set<std::set<action_id_t>> candidate_structures;
  candidate_structures.insert(global_config_set.begin(), global_config_set.end());

  // construct the sequence of set of candidate structures to be used in the final graph search
  std::vector<std::set<std::set<action_id_t>>> candidate_structures_by_segment(end_segment_index_ + 1,
                                                                               candidate_structures);

  // find best solution in the final graph
  double final_soln_cost UNUSED_ATTRIBUTE =
      GraphSolver(planning_context_, forecast_, end_segment_index_, structure_map_, default_segment_cost_,
                  candidate_structures_by_segment, memory_constraint)
          .RecoverShortestPath(best_final_path);
  SELFDRIVING_LOG_DEBUG("[GREEDY-SEQ] final solution cost {}", final_soln_cost);
}

std::set<action_id_t> SequenceTuning::ExtractActionsFromConfigTransition(
    const std::map<pilot::action_id_t, std::unique_ptr<pilot::AbstractAction>> &structure_map,
    const std::set<action_id_t> &start_config, const std::set<action_id_t> &end_config) {
  std::set<action_id_t> actions;
  // include actions that added structures
  for (auto structure_id : end_config)
    if (start_config.find(structure_id) == start_config.end()) actions.emplace(structure_id);

  // include reverse actions for dropped structures
  for (auto structure_id : start_config)
    if (end_config.find(structure_id) == end_config.end()) {
      auto drop_action = structure_map.at(structure_id)->GetReverseActions().at(0);
      actions.emplace(drop_action);
    }

  return actions;
}

void SequenceTuning::ExtractActionsFromConfigPath(
    const std::vector<std::set<action_id_t>> &best_final_config_path,
    std::vector<std::set<std::pair<const std::string, catalog::db_oid_t>>> *best_actions_seq) {
  for (uint64_t config_idx = 1; config_idx < best_final_config_path.size(); config_idx++) {
    std::set<std::pair<const std::string, catalog::db_oid_t>> action_set;
    auto action_id_set = ExtractActionsFromConfigTransition(structure_map_, best_final_config_path.at(config_idx - 1),
                                                            best_final_config_path.at(config_idx));

    for (auto action_id : action_id_set)
      action_set.emplace(structure_map_.at(action_id)->GetSQLCommand(), structure_map_.at(action_id)->GetDatabaseOid());

    best_actions_seq->push_back(std::move(action_set));
  }
}

double SequenceTuning::ConfigTransitionCost(
    const std::map<pilot::action_id_t, std::unique_ptr<pilot::AbstractAction>> &structure_map,
    const std::set<pilot::action_id_t> &start_config, const std::set<pilot::action_id_t> &end_config) {
  auto action_id_set = ExtractActionsFromConfigTransition(structure_map, start_config, end_config);
  double total_cost = 0.0;
  for (auto action_id : action_id_set) total_cost += structure_map.at(action_id)->GetEstimatedElapsedUs();
  return total_cost;
}

void SequenceTuning::MergeConfigs(std::multiset<PathSolution> *global_path_set,
                                  std::set<std::set<action_id_t>> *global_config_set, uint64_t memory_constraint) {
  while (!global_path_set->empty()) {
    // find the least cost path and add its configurations to the global set of configurations
    auto least_cost_path = *(global_path_set->begin());
    // remove the least-cost path
    global_path_set->erase(global_path_set->begin());

    auto best_couple_pair_it = global_path_set->begin();
    double best_union_cost = least_cost_path.path_length_;

    auto config_set = least_cost_path.unique_config_on_path_;
    global_config_set->insert(config_set.begin(), config_set.end());

    // go through each solution in P to see if there's another path where their unioned solution is better
    PathSolution best_merged_solution;
    for (auto path_it = global_path_set->begin(); path_it != global_path_set->end(); ++path_it) {
      PathSolution merged_solution;
      double union_cost = UnionPair(least_cost_path, *path_it, memory_constraint, &merged_solution);

      if (union_cost < best_union_cost) {
        best_couple_pair_it = path_it;
        best_union_cost = union_cost;
        best_merged_solution = std::move(merged_solution);
      }
    }

    // if there's a pair whose unioned solution is better than the least-cost solution alone, remove the other path and
    // add the combined solution to P
    if (best_union_cost < least_cost_path.path_length_) {
      global_path_set->erase(best_couple_pair_it);
      SELFDRIVING_LOG_DEBUG("[GreedySeq] new path added to global_path_set with distance {} number of config {}",
                            best_union_cost, best_merged_solution.unique_config_on_path_.size());
      global_path_set->emplace(std::move(best_merged_solution));

    } else {
      // if there's no combination that's better than the least cost path alone, break out of step 3
      break;
    }
  }
}

}  // namespace noisepage::selfdriving::pilot
