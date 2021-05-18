#pragma once

#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <vector>

#include "common/managed_pointer.h"
#include "self_driving/planning/pilot_util.h"

#define EPSILON 1e-3
#define NULL_ACTION INT32_MAX
#define INIT_DIST INT64_MAX

namespace noisepage::selfdriving {
class Pilot;
class WorkloadForecast;

namespace pilot {

/**
 * The pilot processes the query trace predictions by executing them and extracting pipeline features
 */
class SeqNode {
 public:
  SeqNode(std::set<action_id_t> configuration, double node_cost, uint64_t memory, bool if_source = false)
      : configuration_(configuration),
        node_cost_(node_cost),
        memory_(memory) {
    (void) memory_; // TODO: use memory to check validity;
    if (if_source) best_source_dist_ = 0.0;
  }

  double GetNodeCost() { return node_cost_; }

  common::ManagedPointer<SeqNode> GetBestParent() { return best_parent_; }

  const std::set<action_id_t> &GetConfig() { return configuration_; }

  double GetBestDist() { return best_source_dist_; }

  void RelaxNode(const std::map<pilot::action_id_t, std::unique_ptr<pilot::AbstractAction>> &action_map,
                 const std::vector<std::unique_ptr<SeqNode>> &parents) {
    NOISEPAGE_ASSERT(parents.size() > 0, "cannot relax with zero parents");
    for (auto &parent : parents) {
      double transit_cost = PilotUtil::ConfigTransitionCost(action_map, parent->configuration_, configuration_);
      if (best_parent_ == nullptr || parent->best_source_dist_ + transit_cost + node_cost_ < best_source_dist_) {
        best_parent_ = common::ManagedPointer(parent);
        best_source_dist_ = parent->best_source_dist_ + transit_cost + node_cost_;
      }
    }
  }

 private:
  std::set<action_id_t> configuration_;
  double node_cost_;
  double best_source_dist_{(double) INIT_DIST};
  common::ManagedPointer<SeqNode> best_parent_{nullptr};
  uint64_t memory_;
};
}  // namespace pilot

}  // namespace noisepage::selfdriving
