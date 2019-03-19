#pragma once

#include <memory>
#include <string>
#include "plan_node/abstract_plan_node.h"

namespace terrier::plan_node {

/**
 * Plan node for APPEND.
 */
class AppendPlanNode : public AbstractPlanNode {
 public:
  /**
   * Instantiate a new AppendPlanNode
   */
  AppendPlanNode() = default;

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::APPEND; }

  /**
   * @return debug info
   */
  const std::string GetInfo() const override { return "AppendPlanNode"; }

  DISALLOW_COPY_AND_MOVE(AppendPlanNode);
};

}  // namespace terrier::plan_node
