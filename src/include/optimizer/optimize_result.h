#pragma once

#include <memory>

#include "common/managed_pointer.h"
#include "planner/plannodes/plan_meta_data.h"
#include "planner/plannodes/abstract_plan_node.h"

namespace noisepage::optimizer {

class OptimizeResult {
 public:
  OptimizeResult() {
    plan_meta_data_ = std::make_unique<planner::PlanMetaData>();
  }

  void SetPlanNode(std::unique_ptr<planner::AbstractPlanNode> &&plan_node) {
    plan_node_ = std::move(plan_node);
  }

  common::ManagedPointer<planner::PlanMetaData> GetPlanMetaData() { return common::ManagedPointer(plan_meta_data_); }

  common::ManagedPointer<planner::AbstractPlanNode> GetPlanNode() { return common::ManagedPointer(plan_node_); }

  std::unique_ptr<planner::AbstractPlanNode> &&TakePlanNodeOwnership() { return std::move(plan_node_); }

 private:
  std::unique_ptr<planner::AbstractPlanNode> plan_node_;
  std::unique_ptr<planner::PlanMetaData> plan_meta_data_;
};
}