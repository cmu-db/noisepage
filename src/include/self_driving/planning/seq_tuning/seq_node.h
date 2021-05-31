#pragma once

#include <map>
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "common/managed_pointer.h"
#include "self_driving/planning/pilot_util.h"
#include "self_driving/planning/seq_tuning/sequence_tuning.h"

namespace noisepage::selfdriving {

namespace pilot {

/**
 * The SeqNode class represents a configuration (set of structures/indices) associated with a forecasted segment.
 * It also stores the cost of queries in the segment under the configuration.
 */
class SeqNode {
  /* Initial distance assigned to nodes that are not the source */
  static constexpr double INIT_DIST = static_cast<double>(INT64_MAX);

 public:
  /**
   * Constructing a sequence node representing a set of statements (segment) with a configuration (set of structures).
   * @param configuration set of create index action id
   * @param node_cost cost of executing the statements under the config
   * @param if_source if the node is a source node (default to have distance 0)
   */
  explicit SeqNode(std::set<action_id_t> configuration, double node_cost, bool if_source = false)
      : configuration_(std::move(configuration)), node_cost_(node_cost) {
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
      double transit_cost = SequenceTuning::ConfigTransitionCost(structure_map, parent->configuration_, configuration_);
      if (best_parent_ == nullptr || parent->best_source_dist_ + transit_cost + node_cost_ < best_source_dist_) {
        best_parent_ = common::ManagedPointer(parent);
        best_source_dist_ = parent->best_source_dist_ + transit_cost + node_cost_;
      }
    }
  }

 private:
  std::set<action_id_t> configuration_;
  double node_cost_;
  double best_source_dist_{INIT_DIST};
  common::ManagedPointer<SeqNode> best_parent_{nullptr};
};
}  // namespace pilot

}  // namespace noisepage::selfdriving
