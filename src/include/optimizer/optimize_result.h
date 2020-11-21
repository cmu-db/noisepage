#pragma once

#include <memory>
#include <utility>

#include "common/managed_pointer.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/plan_meta_data.h"

namespace noisepage::optimizer {

/**
 * OptimizeResult stores the optimization result for an op_tree, including the root plan node and the plan meta data
 */
class OptimizeResult {
 public:
  OptimizeResult() { plan_meta_data_ = std::make_unique<planner::PlanMetaData>(); }

  /**
   * Set plan node after buildPlanNode
   * @param plan_node generated plan node
   */
  void SetPlanNode(std::unique_ptr<planner::AbstractPlanNode> &&plan_node) { plan_node_ = std::move(plan_node); }

  /**
   * @return plan meta data
   */
  common::ManagedPointer<planner::PlanMetaData> GetPlanMetaData() { return common::ManagedPointer(plan_meta_data_); }

  /**
   * @return root plan node
   */
  common::ManagedPointer<planner::AbstractPlanNode> GetPlanNode() { return common::ManagedPointer(plan_node_); }

  /**
   * Get the ownership of plan node
   * @return root plan node
   */
  std::unique_ptr<planner::AbstractPlanNode> &&TakePlanNodeOwnership() { return std::move(plan_node_); }

 private:
  std::unique_ptr<planner::AbstractPlanNode> plan_node_;
  std::unique_ptr<planner::PlanMetaData> plan_meta_data_;
};
}  // namespace noisepage::optimizer
