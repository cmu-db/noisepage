#pragma once

#include "abstract_plan_node.h"
#include "common/internal_types.h"

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
  inline PlanNodeType GetPlanNodeType() const { return PlanNodeType::APPEND; }

  /**
   * @return debug info
   */
  const std::string GetInfo() const { return "Append Plan Node"; }

 private:
  DISALLOW_COPY_AND_MOVE(AppendPlanNode);
};

}  // namespace terrier::plan_node