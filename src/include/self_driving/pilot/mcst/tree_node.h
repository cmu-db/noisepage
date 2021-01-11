#pragma once

#include <map>
#include <vector>
#include <unordered_set>

#include "common/managed_pointer.h"
#include "self_driving/pilot/action/action_defs.h"

#define NULL_ACTION INT32_MAX

namespace noisepage::selfdriving {
class Pilot;
class WorkloadForecast;

namespace pilot {
class AbstractAction;

/**
 * The pilot processes the query trace predictions by executing them and extracting pipeline features
 */
class TreeNode {
 public:
  TreeNode(common::ManagedPointer<TreeNode> parent, action_id_t current_action, uint64_t cost);

  action_id_t GetCurrentAction() { return current_action_; }

  uint64_t GetDepth() { return depth_; }

  common::ManagedPointer<TreeNode> GetParent() { return parent_; };

  uint64_t GetNumberOfChildren() { return children_.size(); }
  void UpdateVisits(uint64_t add_num) { number_of_visits_ += add_num; }

  bool IsLeaf() { return is_leaf_; }

  common::ManagedPointer<TreeNode> SampleChild();

  /**
   * @return child with least cost
   */
  common::ManagedPointer<TreeNode> BestChild();

  void ChildrenRollout(common::ManagedPointer<Pilot> pilot_,
                       common::ManagedPointer<WorkloadForecast> forecast,
                       uint64_t start_segment_index,
                       uint64_t end_segment_index,
                       const std::vector<std::vector<uint64_t>> &db_oids,
                       const std::map<action_id_t, std::unique_ptr<AbstractAction>> &action_map_,
                       const std::unordered_set<action_id_t> &candidate_actions_);

 private:
  bool is_leaf_;
  uint64_t depth_;
  uint64_t number_of_visits_;
  common::ManagedPointer<TreeNode> parent_;
  std::vector<std::unique_ptr<TreeNode>> children_;
  action_id_t current_action_{INT32_MAX};
  uint64_t cost_{UINT64_MAX};

};
}

}  // namespace noisepage::selfdriving::pilot
