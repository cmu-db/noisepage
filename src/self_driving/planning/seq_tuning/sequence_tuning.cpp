#include "self_driving/planning/seq_tuning/sequence_tuning.h"

#include <map>
#include <vector>

#include "common/managed_pointer.h"
#include "loggers/selfdriving_logger.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "self_driving/planning/action/generators/change_knob_action_generator.h"
#include "self_driving/planning/action/generators/index_action_generator.h"
#include "self_driving/planning/pilot_util.h"
#include "self_driving/planning/seq_tuning/graph_solver.h"
#include "transaction/transaction_manager.h"

namespace noisepage::selfdriving::pilot {

SequenceTuning::SequenceTuning(common::ManagedPointer<Pilot> pilot,
                               common::ManagedPointer<selfdriving::WorkloadForecast> forecast,
                               uint64_t end_segment_index)
    : pilot_(pilot), forecast_(forecast), end_segment_index_(end_segment_index) {
  transaction::TransactionContext *txn = pilot->txn_manager_->BeginTransaction();

  std::vector<std::unique_ptr<planner::AbstractPlanNode>> plans;
  // vector of query plans that the search tree is responsible for
  PilotUtil::GetQueryPlans(pilot, common::ManagedPointer(forecast_), end_segment_index, txn, &plans);

  std::vector<action_id_t> candidate_actions;
  // populate structure_map_, candidate_structures_
  IndexActionGenerator().GenerateActions(plans, pilot->settings_manager_, &structure_map_, &candidate_actions);

  for (auto &action : candidate_actions)
    if (structure_map_.at(action)->GetSQLCommand().rfind("create index", 0) == 0) {
      candidate_structures_.emplace_back(action);
      SELFDRIVING_LOG_DEBUG("Candidate structure: ID {} Command {}", action,
                            structure_map_.at(action)->GetSQLCommand());
    }

  for (const auto &it UNUSED_ATTRIBUTE : structure_map_) {
    SELFDRIVING_LOG_DEBUG("Generated action: ID {} Command {}", it.first, it.second->GetSQLCommand());
  }

  pilot->txn_manager_->Abort(txn);

  std::vector<double> default_segment_cost;
  // first compute the cost of each segment when no action is applied
  for (uint64_t segment_index = 0; segment_index <= end_segment_index_; segment_index++) {
    default_segment_cost.emplace_back(PilotUtil::ComputeCost(pilot_, forecast_, segment_index, segment_index));
  }
  default_segment_cost_ = default_segment_cost;
}

void SequenceTuning::BestAction(
    std::vector<std::set<std::pair<const std::string, catalog::db_oid_t>>> *best_actions_seq,
    uint64_t memory_constraint) {
  // cost-based pruning for each individual action
  std::map<action_id_t, std::tuple<std::vector<std::set<action_id_t>>, std::set<std::set<action_id_t>>, double>>
      best_path_for_structure;

  for (auto structure_id : candidate_structures_) {
    std::vector<std::set<action_id_t>> best_path_for_curr_structure;
    std::set<std::set<action_id_t>> best_config_set_for_curr_action;

    std::vector<std::set<action_id_t>> singleton_action = {{structure_id}};
    std::vector<std::vector<std::set<action_id_t>>> singleton_action_repeated(end_segment_index_ + 1, singleton_action);

    double best_path_cost = GraphSolver(pilot_, forecast_, end_segment_index_, structure_map_, default_segment_cost_,
                                        singleton_action_repeated, 0.0)
                                .RecoverShortestPath(&best_path_for_curr_structure, &best_config_set_for_curr_action);

    SELFDRIVING_LOG_INFO("[cost-based pruning] for structure \"{}\" finds best path distance {} with {} configs",
                         structure_map_.at(structure_id)->GetSQLCommand(), best_path_cost,
                         best_config_set_for_curr_action.size());

    best_path_for_structure.emplace(
        structure_id, std::make_tuple(best_path_for_curr_structure, best_config_set_for_curr_action, best_path_cost));
  }

  // since we only have one sequence (no split step), directly apply greedy-sequence algo to merge individual paths to
  // get the final solution, (no merge step at the end)
  std::vector<std::set<action_id_t>> best_final_config_path;
  GreedySeq(best_path_for_structure, &best_final_config_path);
  ExtractActionsFromConfigPath(best_final_config_path, best_actions_seq);
}

double SequenceTuning::UnionPair(const std::vector<std::set<action_id_t>> &seq_one,
                                 const std::vector<std::set<action_id_t>> &seq_two,
                                 std::vector<std::set<action_id_t>> *merged_solution,
                                 std::set<std::set<action_id_t>> *merged_config_set) {
  std::vector<std::vector<std::set<action_id_t>>> candidate_structures_by_segment;

  NOISEPAGE_ASSERT(seq_one.size() == seq_two.size(), "UnionPair requires two sequences of same length");

  for (uint64_t structure_idx = 0; structure_idx < seq_one.size(); structure_idx++) {
    std::vector<std::set<action_id_t>> curr_level;
    curr_level.push_back(seq_one.at(structure_idx));
    curr_level.push_back(seq_two.at(structure_idx));

    std::set<action_id_t> unioned_config;
    unioned_config.insert(seq_one.at(structure_idx).begin(), seq_one.at(structure_idx).end());
    unioned_config.insert(seq_two.at(structure_idx).begin(), seq_two.at(structure_idx).end());
    curr_level.push_back(unioned_config);
    candidate_structures_by_segment.push_back(std::move(curr_level));
  }

  double best_unioned_dist = GraphSolver(pilot_, forecast_, end_segment_index_, structure_map_, default_segment_cost_,
                                         candidate_structures_by_segment, 0.0)
                                 .RecoverShortestPath(merged_solution, merged_config_set);
  return best_unioned_dist;
}

void SequenceTuning::GreedySeq(
    const std::map<action_id_t, std::tuple<std::vector<std::set<action_id_t>>, std::set<std::set<action_id_t>>, double>>
        &best_path_for_structure,
    std::vector<std::set<action_id_t>> *best_final_config_path) {
  // initialize C to contain all (binary) configs for each potential structure
  std::set<std::set<action_id_t>> global_config_set;
  // initialize global_path_set as paths found in cost-based pruning
  std::set<std::pair<double, uint64_t>> global_path_set;
  std::vector<std::tuple<std::vector<std::set<action_id_t>>, std::set<std::set<action_id_t>>, double>> all_paths;

  uint64_t ct = 0;
  for (auto const &path_it : best_path_for_structure) {
    auto const &config_set = std::get<1>(path_it.second);
    global_config_set.insert(config_set.begin(), config_set.end());
    all_paths.push_back(path_it.second);
    global_path_set.emplace(std::get<2>(path_it.second), ct);
    ct++;
  }

  MergeConfigs(&global_path_set, &all_paths, &global_config_set);

  std::vector<std::set<action_id_t>> candidate_structures;
  candidate_structures.insert(candidate_structures.begin(), global_config_set.begin(), global_config_set.end());

  std::vector<std::vector<std::set<action_id_t>>> candidate_structures_by_segment(end_segment_index_ + 1,
                                                                                  candidate_structures);

  double final_soln_cost UNUSED_ATTRIBUTE = GraphSolver(pilot_, forecast_, end_segment_index_, structure_map_,
                                                        default_segment_cost_, candidate_structures_by_segment, 0.0)
                                                .RecoverShortestPath(best_final_config_path, nullptr);
  SELFDRIVING_LOG_DEBUG("[GREEDY-SEQ] final solution cost {}", final_soln_cost);
}

void SequenceTuning::ExtractActionsFromConfigPath(
    const std::vector<std::set<action_id_t>> &best_final_config_path,
    std::vector<std::set<std::pair<const std::string, catalog::db_oid_t>>> *best_actions_seq) {
  for (uint64_t config_idx = 1; config_idx < best_final_config_path.size(); config_idx++) {
    std::set<std::pair<const std::string, catalog::db_oid_t>> action_set;
    for (auto structure_id : best_final_config_path.at(config_idx))
      if (best_final_config_path.at(config_idx - 1).find(structure_id) ==
          best_final_config_path.at(config_idx - 1).end())
        action_set.emplace(structure_map_.at(structure_id)->GetSQLCommand(),
                           structure_map_.at(structure_id)->GetDatabaseOid());

    for (auto structure_id : best_final_config_path.at(config_idx - 1))
      if (best_final_config_path.at(config_idx).find(structure_id) == best_final_config_path.at(config_idx).end()) {
        auto drop_action = structure_map_.at(structure_id)->GetReverseActions().at(0);
        action_set.emplace(structure_map_.at(drop_action)->GetSQLCommand(),
                           structure_map_.at(drop_action)->GetDatabaseOid());
      }
    best_actions_seq->push_back(std::move(action_set));
  }
}

void SequenceTuning::MergeConfigs(
    std::set<std::pair<double, uint64_t>> *global_path_set,
    std::vector<std::tuple<std::vector<std::set<action_id_t>>, std::set<std::set<action_id_t>>, double>> *all_paths,
    std::set<std::set<action_id_t>> *global_config_set) {
  while (!global_path_set->empty()) {
    uint64_t least_cost_index = global_path_set->begin()->second;
    double least_cost = global_path_set->begin()->first;
    auto best_couple_pair = *(global_path_set->begin());
    double best_union_cost = least_cost;

    global_path_set->erase(global_path_set->begin());

    auto config_set = std::get<1>(all_paths->at(least_cost_index));
    global_config_set->insert(config_set.begin(), config_set.end());

    std::vector<std::set<action_id_t>> best_merged_solution;
    std::set<std::set<action_id_t>> best_merged_config_set;

    for (auto path_it : *global_path_set) {
      std::vector<std::set<action_id_t>> merged_solution;
      std::set<std::set<action_id_t>> merged_config_set;
      uint64_t second_index = path_it.second;
      double union_cost = UnionPair(std::get<0>(all_paths->at(least_cost_index)),
                                    std::get<0>(all_paths->at(second_index)), &merged_solution, &merged_config_set);

      if (union_cost < best_union_cost) {
        best_couple_pair = path_it;
        best_union_cost = union_cost;
        best_merged_solution = merged_solution;
        best_merged_config_set = merged_config_set;
      }
    }

    if (best_union_cost < least_cost) {
      global_path_set->erase(best_couple_pair);
      all_paths->emplace_back(best_merged_solution, best_merged_config_set, best_union_cost);
      global_path_set->emplace(best_union_cost, all_paths->size() - 1);

      SELFDRIVING_LOG_DEBUG("[GreedySeq] new path added to global_path_set with distance {} number of config {}",
                            best_union_cost, best_merged_config_set.size());
    } else {
      // if there's no combination that's better than the least cost path alone, break out of step 3
      break;
    }
  }
}

}  // namespace noisepage::selfdriving::pilot
