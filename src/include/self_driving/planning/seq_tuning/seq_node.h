#pragma once

#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "common/managed_pointer.h"
#include "self_driving/planning/mcts/action_state.h"
#include "self_driving/planning/pilot_util.h"

namespace noisepage::selfdriving {
class Pilot;
class WorkloadForecast;

namespace pilot {

/**
 * The pilot processes the query trace predictions by executing them and extracting pipeline features
 */
class SeqNode {
 protected:
  /* Initial distance assigned to nodes that are not the source */
  static constexpr double INIT_DIST = static_cast<double>(INT64_MAX);

 public:
  /**
   * Constructing a sequence node representing a set of statements (segment) with a configuration (set of structures).
   * @param configuration set of create index action id
   * @param node_cost cost of executing the statements under the config
   * @param memory number of bytes consumed by the config
   * @param action_state
   * @param if_source if the node is a source node (default to have distance 0)
   */
  explicit SeqNode(std::set<action_id_t> configuration, double node_cost, uint64_t memory, ActionState action_state,
                   bool if_source = false)
      : configuration_(std::move(configuration)),
        node_cost_(node_cost),
        memory_(memory),
        best_action_state_(std::move(action_state)) {
    (void)memory_;  // TODO(Katrina): Add the memory information to the action recording table
    if (if_source) best_source_dist_ = 0.0;
  }

  /**
   * @return The cost of the node's statements under the config of this node
   */
  double GetNodeCost() { return node_cost_; }

  /**
   * @return Best parent on shortest path from source.
   */
  common::ManagedPointer<SeqNode> GetBestParent() { return best_parent_; }

  /**
   * @return Configuration (set of create index actions)
   */
  const std::set<action_id_t> &GetConfig() { return configuration_; }

  /**
   * @return Shortest distance from source so far
   */
  double GetBestDist() { return best_source_dist_; }

  /**
   * Relax all incoming edges of a node by considering if continuing the path from each of its parent would yield
   * a shorter path.
   * @param structure_map map containing information about the indexes/structures
   * @param parents parents of this node, whose edges are to be relaxed
   */
  void RelaxNode(const std::map<pilot::action_id_t, std::unique_ptr<pilot::AbstractAction>> &structure_map,
                 const std::vector<std::unique_ptr<SeqNode>> &parents) {
    NOISEPAGE_ASSERT(!parents.empty(), "cannot relax with zero parents");
    for (auto &parent : parents) {
      double transit_cost = PilotUtil::ConfigTransitionCost(structure_map, parent->configuration_, configuration_);
      if (best_parent_ == nullptr || parent->best_source_dist_ + transit_cost + node_cost_ < best_source_dist_) {
        best_parent_ = common::ManagedPointer(parent);
        best_source_dist_ = parent->best_source_dist_ + transit_cost + node_cost_;
      }
    }
  }

 private:
  std::set<action_id_t> configuration_;
  double node_cost_;
  double best_source_dist_{static_cast<double>(INIT_DIST)};
  common::ManagedPointer<SeqNode> best_parent_{nullptr};
  uint64_t memory_;
  ActionState best_action_state_;
};
}  // namespace pilot

}  // namespace noisepage::selfdriving
