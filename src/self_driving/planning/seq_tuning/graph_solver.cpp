#include "self_driving/planning/seq_tuning/graph_solver.h"

#include <cmath>
#include <random>

#include "loggers/selfdriving_logger.h"
#include "self_driving/forecasting/workload_forecast.h"
#include "self_driving/planning/action/abstract_action.h"
#include "self_driving/planning/memory_info.h"
#include "self_driving/planning/pilot.h"
#include "self_driving/planning/pilot_util.h"
#include "self_driving/planning/seq_tuning/path_solution.h"

namespace noisepage::selfdriving::pilot {

GraphSolver::GraphSolver(common::ManagedPointer<Pilot> pilot,
                         common::ManagedPointer<selfdriving::WorkloadForecast> forecast, uint64_t end_segment_index,
                         const std::map<action_id_t, std::unique_ptr<AbstractAction>> &structure_map,
                         const std::vector<double> &default_segment_cost,
                         const std::vector<std::set<std::set<action_id_t>>> &candidate_configurations_by_segment,
                         uint64_t memory_constraint) {
  // build the graph and compute shortest path simultaneously

  ActionState action_state;
  action_state.SetIntervals(0, end_segment_index);

  source_level_.push_back(std::make_unique<SeqNode>(std::set<action_id_t>(), 0.0, 0.0, action_state, true));

  auto new_action_state = action_state;
  for (uint64_t segment_index = 0; segment_index <= end_segment_index; segment_index++) {
    new_action_state.SetIntervals(segment_index + 1, end_segment_index);

    // initialize nodes with no action applied
    std::vector<std::unique_ptr<SeqNode>> curr_level;
    curr_level.push_back(std::make_unique<SeqNode>(std::set<action_id_t>(), default_segment_cost.at(segment_index), 0.0,
                                                   new_action_state));
    nodes_by_segment_index_.push_back(std::move(curr_level));

    const std::vector<std::unique_ptr<SeqNode>> &parent_level =
        segment_index == 0 ? source_level_ : nodes_by_segment_index_.at(segment_index - 1);

    nodes_by_segment_index_.at(segment_index).back()->RelaxNode(structure_map, parent_level);

    SELFDRIVING_LOG_DEBUG("[InitGraph] level {}, config_set {{}}, node_cost {}, best_dist {}", segment_index,
                          default_segment_cost.at(segment_index),
                          nodes_by_segment_index_.at(segment_index).back()->GetBestDist());

    // initialize nodes with each of the config
    for (auto const &config_set : candidate_configurations_by_segment.at(segment_index)) {
      if (config_set.empty() || !IsValidConfig(pilot, structure_map, config_set)) continue;

      new_action_state.SetIntervals(segment_index + 1, end_segment_index);

      for (auto const action : config_set) structure_map.at(action)->ModifyActionState(&new_action_state);

      // Compute memory consumption
      bool satisfy_memory_constraint = true;
      // We may apply actions to reduce memory consumption in future, so we only need to evaluate the memory constraint
      size_t memory =
          PilotUtil::CalculateMemoryConsumption(pilot->GetMemoryInfo(), new_action_state, segment_index, structure_map);
      if (memory > memory_constraint) satisfy_memory_constraint = false;

      double node_cost = MEMORY_CONSUMPTION_VIOLATION_COST;

      if (satisfy_memory_constraint) {
        node_cost = ComputeConfigCost(pilot, forecast, structure_map, config_set, segment_index);
      }

      nodes_by_segment_index_.at(segment_index)
          .push_back(std::make_unique<SeqNode>(config_set, node_cost, memory, new_action_state));
      nodes_by_segment_index_.at(segment_index).back()->RelaxNode(structure_map, parent_level);

      SELFDRIVING_LOG_DEBUG("[InitGraph] level {}, config_set {}, node_cost {}, best_dist {}", segment_index,
                            PilotUtil::ConfigToString(config_set), node_cost,
                            nodes_by_segment_index_.at(segment_index).back()->GetBestDist());

      for (auto const action : config_set) {
        // clean up by applying one reverse action to undo the above
        auto rev_actions = structure_map.at(action)->GetReverseActions();
        structure_map.at(rev_actions[0])->ModifyActionState(&new_action_state);
      }
    }
  }

  // add dummy destination node, relaxing it computes the optimal path
  dest_node_ = std::make_unique<SeqNode>(std::set<action_id_t>(), 0.0, 0.0, action_state);
  dest_node_->RelaxNode(structure_map, nodes_by_segment_index_.back());
}

double GraphSolver::ComputeConfigCost(common::ManagedPointer<Pilot> pilot,
                                      common::ManagedPointer<selfdriving::WorkloadForecast> forecast,
                                      const std::map<action_id_t, std::unique_ptr<AbstractAction>> &structure_map,
                                      const std::set<action_id_t> &config_set, uint64_t segment_index) {
  for (auto const action : config_set) {
    PilotUtil::ApplyAction(pilot, structure_map.at(action)->GetSQLCommand(), structure_map.at(action)->GetDatabaseOid(),
                           Pilot::WHAT_IF);
  }

  // if configuration is valid, include it as a node in the graph
  auto node_cost = PilotUtil::ComputeCost(pilot, forecast, segment_index, segment_index);

  for (auto const action : config_set) {
    // clean up by applying one reverse action to undo the above
    auto rev_actions = structure_map.at(action)->GetReverseActions();
    PilotUtil::ApplyAction(pilot, structure_map.at(rev_actions[0])->GetSQLCommand(),
                           structure_map.at(rev_actions[0])->GetDatabaseOid(), Pilot::WHAT_IF);
  }

  return node_cost;
}

bool GraphSolver::IsValidConfig(common::ManagedPointer<Pilot> pilot,
                                const std::map<action_id_t, std::unique_ptr<AbstractAction>> &structure_map,
                                const std::set<action_id_t> &config_set) {
  for (auto candidate_action : config_set) {
    if (!structure_map.at(candidate_action)->IsValid()) {
      return false;
    }
  }
  return true;
}

double GraphSolver::RecoverShortestPath(PathSolution *merged_solution) {
  auto curr_node = dest_node_->GetBestParent();
  // get all configs on best path
  SELFDRIVING_LOG_DEBUG("PRINTING Shortest Config Path");

  auto best_config_path = &(merged_solution->config_on_path);
  auto best_config_set = &(merged_solution->unique_config_on_path);

  while (curr_node != nullptr) {
    best_config_path->push_back(curr_node->GetConfig());
    if (best_config_set != nullptr) best_config_set->emplace(curr_node->GetConfig());

    SELFDRIVING_LOG_DEBUG(" Reversed LEVEL {} : config {}, node_cost {}, best_dist {}", best_config_path->size(),
                          PilotUtil::ConfigToString(best_config_path->back()), curr_node->GetNodeCost(),
                          curr_node->GetBestDist());

    curr_node = curr_node->GetBestParent();
  }

  std::reverse(best_config_path->begin(), best_config_path->end());

  return dest_node_->GetBestDist();
}

}  // namespace noisepage::selfdriving::pilot
