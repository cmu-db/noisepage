
#include "self_driving/planning/seq_tuning/graph_solver.h"

#include <cmath>
#include <random>

#include "loggers/selfdriving_logger.h"
#include "self_driving/forecasting/workload_forecast.h"
#include "self_driving/planning/action/abstract_action.h"
#include "self_driving/planning/pilot.h"
#include "self_driving/planning/pilot_util.h"

#define EPSILON 1e-3

namespace noisepage::selfdriving::pilot {

GraphSolver::GraphSolver(common::ManagedPointer<Pilot> pilot,
                         common::ManagedPointer<selfdriving::WorkloadForecast> forecast, uint64_t end_segment_index,
                         const std::map<action_id_t, std::unique_ptr<AbstractAction>> &structure_map,
                         std::vector<double> default_segment_cost,
                         std::vector<std::vector<std::set<action_id_t>>> candidate_configurations_by_segment,
                         uint64_t memory_constraint) {
  // build the graph and compute shortest path simultaneously
  // TODO: populate all memory attributes correctly

  source_level_.push_back(std::make_unique<SeqNode>(std::set<action_id_t>(), 0.0, 0.0, true));

  // initialize nodes with no action applied;
  std::vector<std::unique_ptr<SeqNode>> curr_level;
  for (auto segment_cost : default_segment_cost) {
    curr_level.push_back(std::make_unique<SeqNode>(std::set<action_id_t>(), segment_cost, 0.0));

    nodes_by_segment_index_.push_back(std::move(curr_level));
  }

  for (uint64_t segment_index = 0; segment_index <= end_segment_index; segment_index ++) {
    const std::vector<std::unique_ptr<SeqNode>> &parent_level =
        segment_index == 0 ? source_level_ : nodes_by_segment_index_.at(segment_index - 1);
    nodes_by_segment_index_.at(segment_index).back()->RelaxNode(structure_map, parent_level);

    for (auto config_set : candidate_configurations_by_segment.at(segment_index)) {
      if (config_set.empty()) continue;
      std::set<action_id_t> actions_applied;
      bool is_valid_config = IsValidConfig(pilot, structure_map, config_set, &actions_applied, memory_constraint);

      if (is_valid_config) {
        // if configuration is valid, include it as a node in the graph
        double node_cost = PilotUtil::ComputeCost(pilot, forecast, segment_index, segment_index);
        nodes_by_segment_index_.at(segment_index).push_back(std::make_unique<SeqNode>(config_set, node_cost, 0.0));

        nodes_by_segment_index_.at(segment_index).back()->RelaxNode(structure_map, parent_level);

        std::string set_string = "{";
        for (auto action : config_set) {
          set_string += fmt::format(" action_id {} ", action);
        }
        set_string += "}";

        SELFDRIVING_LOG_DEBUG("[InitGraph] level {}, config_set {}, node_cost {}, best_dist {}", segment_index,
                             set_string, node_cost, nodes_by_segment_index_.at(segment_index).back()->GetBestDist());
      }

      for (auto action : actions_applied) {
        // clean up by applying one reverse action to undo the above
        auto rev_actions = structure_map.at(action)->GetReverseActions();
        PilotUtil::ApplyAction(pilot, structure_map.at(rev_actions[0])->GetSQLCommand(),
                               structure_map.at(rev_actions[0])->GetDatabaseOid(), Pilot::WHAT_IF);
      }
    }
  }

  // add dummy destination node, relaxing it computes the optimal path
  dest_node_ = std::make_unique<SeqNode>(std::set<action_id_t>(), 0.0, 0.0);
  dest_node_->RelaxNode(structure_map, nodes_by_segment_index_.back());
}

bool GraphSolver::IsValidConfig(common::ManagedPointer<Pilot> pilot,
                                const std::map<action_id_t, std::unique_ptr<AbstractAction>> &structure_map,
                                const std::set<action_id_t> &config_set,
                                std::set<action_id_t> *actions_applied, uint64_t memory_constraint) {
  bool is_valid_config = true;
  (void) memory_constraint; // TODO: use memory constraint to check if action valid;
  for (auto candidate_action : config_set) {
    if (structure_map.at(candidate_action)->IsValid()) {
      PilotUtil::ApplyAction(pilot, structure_map.at(candidate_action)->GetSQLCommand(),
                             structure_map.at(candidate_action)->GetDatabaseOid(), Pilot::WHAT_IF);
      actions_applied->emplace(candidate_action);
    } else {
      is_valid_config = false;
      break;
    }
  }
  return is_valid_config;
}

double GraphSolver::RecoverShortestPath(std::vector<std::set<action_id_t>> *best_config_path,
                                        std::set<std::set<action_id_t>> *best_config_set) {
  auto curr_node = dest_node_->GetBestParent();

  // get all configs on best path
  auto ct = 0;
  SELFDRIVING_LOG_DEBUG("PRINTING Shortest Config Path");

  while (curr_node != nullptr) {
    best_config_path->push_back(curr_node->GetConfig());
    if (best_config_set != nullptr) best_config_set->emplace(curr_node->GetConfig());

    std::string set_string = "{";
    for (auto action : best_config_path->back()) {
      set_string += fmt::format(" action_id {} ", action);
    }
    set_string += "}";
    SELFDRIVING_LOG_DEBUG(" Reversed LEVEL {} : config {}, node_cost {}, best_dist {}", ct, set_string,
                          curr_node->GetNodeCost(), curr_node->GetBestDist());
    ct ++;

    curr_node = curr_node->GetBestParent();
  }

  std::reverse(best_config_path->begin(), best_config_path->end());

  return dest_node_->GetBestDist();
}

}  // namespace noisepage::selfdriving::pilot
