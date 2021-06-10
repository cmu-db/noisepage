#include "self_driving/planning/seq_tuning/graph_solver.h"

#include "loggers/selfdriving_logger.h"
#include "self_driving/forecasting/workload_forecast.h"
#include "self_driving/planning/action/abstract_action.h"
#include "self_driving/planning/mcts/action_state.h"
#include "self_driving/planning/pilot.h"
#include "self_driving/planning/pilot_util.h"
#include "self_driving/planning/planning_context.h"
#include "self_driving/planning/seq_tuning/path_solution.h"

namespace noisepage::selfdriving::pilot {

GraphSolver::GraphSolver(const PlanningContext &planning_context,
                         common::ManagedPointer<selfdriving::WorkloadForecast> forecast, uint64_t end_segment_index,
                         const std::map<action_id_t, std::unique_ptr<AbstractAction>> &structure_map,
                         const std::vector<double> &default_segment_cost,
                         const std::vector<std::set<std::set<action_id_t>>> &candidate_configurations_by_segment,
                         uint64_t memory_constraint) {
  // build the graph and compute shortest path simultaneously

  source_level_.push_back(std::make_unique<SeqNode>(std::set<action_id_t>(), 0.0, true));

  ActionState action_state;
  for (uint64_t segment_index = 0; segment_index <= end_segment_index; segment_index++) {
    action_state.SetIntervals(segment_index + 1, end_segment_index);

    // initialize nodes with no action applied
    std::vector<std::unique_ptr<SeqNode>> curr_level;
    curr_level.push_back(std::make_unique<SeqNode>(std::set<action_id_t>(), default_segment_cost.at(segment_index)));
    nodes_by_segment_index_.push_back(std::move(curr_level));

    const std::vector<std::unique_ptr<SeqNode>> &parent_level =
        segment_index == 0 ? source_level_ : nodes_by_segment_index_.at(segment_index - 1);

    nodes_by_segment_index_.at(segment_index).back()->RelaxNode(structure_map, parent_level);

    SELFDRIVING_LOG_DEBUG("[InitGraph] level {}, config_set {{}}, node_cost {}, best_dist {}", segment_index,
                          default_segment_cost.at(segment_index),
                          nodes_by_segment_index_.at(segment_index).back()->GetBestDist());

    // initialize nodes with each of the config
    for (auto const &config_set : candidate_configurations_by_segment.at(segment_index)) {
      if (config_set.empty() || !IsValidConfig(planning_context, structure_map, config_set)) continue;

      action_state.SetIntervals(segment_index + 1, end_segment_index);

      for (auto const action : config_set) structure_map.at(action)->ModifyActionState(&action_state);

      // Compute memory consumption
      bool satisfy_memory_constraint = true;
      // We may apply actions to reduce memory consumption in future, so we only need to evaluate the memory constraint
      size_t memory = PilotUtil::CalculateMemoryConsumption(planning_context.GetMemoryInfo(), action_state,
                                                            segment_index, structure_map);
      if (memory > memory_constraint) satisfy_memory_constraint = false;

      double node_cost = MEMORY_CONSUMPTION_VIOLATION_COST;

      if (satisfy_memory_constraint) {
        node_cost = ComputeConfigCost(planning_context, forecast, structure_map, config_set, segment_index);
      }

      nodes_by_segment_index_.at(segment_index).push_back(std::make_unique<SeqNode>(config_set, node_cost));
      nodes_by_segment_index_.at(segment_index).back()->RelaxNode(structure_map, parent_level);

      SELFDRIVING_LOG_DEBUG("[InitGraph] level {}, config_set {}, node_cost {}, best_dist {}", segment_index,
                            PilotUtil::ConfigToString(config_set), node_cost,
                            nodes_by_segment_index_.at(segment_index).back()->GetBestDist());

      for (auto const action : config_set) {
        // clean up by applying one reverse action to undo the above
        auto rev_action = structure_map.at(action)->GetReverseActions().at(0);
        structure_map.at(rev_action)->ModifyActionState(&action_state);
      }
    }
  }

  // add dummy destination node, relaxing it computes the optimal path
  dest_node_ = std::make_unique<SeqNode>(std::set<action_id_t>(), 0.0);
  dest_node_->RelaxNode(structure_map, nodes_by_segment_index_.back());
}

double GraphSolver::ComputeConfigCost(const PlanningContext &planning_context,
                                      common::ManagedPointer<selfdriving::WorkloadForecast> forecast,
                                      const std::map<action_id_t, std::unique_ptr<AbstractAction>> &structure_map,
                                      const std::set<action_id_t> &config_set, uint64_t segment_index) {
  for (auto const action : config_set) {
    PilotUtil::ApplyAction(planning_context, structure_map.at(action)->GetSQLCommand(),
                           structure_map.at(action)->GetDatabaseOid(), Pilot::WHAT_IF);
  }

  // if configuration is valid, include it as a node in the graph
  auto node_cost = PilotUtil::ComputeCost(planning_context, forecast, segment_index, segment_index);

  for (auto const action : config_set) {
    // clean up by applying one reverse action to undo the above
    auto rev_action = structure_map.at(action)->GetReverseActions().at(0);
    PilotUtil::ApplyAction(planning_context, structure_map.at(rev_action)->GetSQLCommand(),
                           structure_map.at(rev_action)->GetDatabaseOid(), Pilot::WHAT_IF);
  }

  return node_cost;
}

bool GraphSolver::IsValidConfig(const PlanningContext &planning_context,
                                const std::map<action_id_t, std::unique_ptr<AbstractAction>> &structure_map,
                                const std::set<action_id_t> &config_set) {
  for (auto candidate_action : config_set) {
    if (!structure_map.at(candidate_action)->IsValid()) {
      return false;
    }
  }
  return true;
}

double GraphSolver::RecoverShortestPath(PathSolution *shortest_path) {
  auto curr_node = dest_node_->GetBestParent();
  // get all configs on best path
  SELFDRIVING_LOG_DEBUG("PRINTING Shortest Config Path");

  auto best_config_path = &(shortest_path->config_on_path_);
  auto best_config_set = &(shortest_path->unique_config_on_path_);

  while (curr_node != nullptr) {
    best_config_path->push_back(curr_node->GetConfig());
    if (best_config_set != nullptr) best_config_set->emplace(curr_node->GetConfig());

    SELFDRIVING_LOG_DEBUG(" Reversed LEVEL {} : config {}, node_cost {}, best_dist {}", best_config_path->size(),
                          PilotUtil::ConfigToString(best_config_path->back()), curr_node->GetNodeCost(),
                          curr_node->GetBestDist());

    curr_node = curr_node->GetBestParent();
  }

  std::reverse(best_config_path->begin(), best_config_path->end());
  shortest_path->path_length_ = dest_node_->GetBestDist();

  return dest_node_->GetBestDist();
}

}  // namespace noisepage::selfdriving::pilot
