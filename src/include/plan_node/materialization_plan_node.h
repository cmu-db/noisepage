#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "plan_node/abstract_plan_node.h"
#include "plan_node/output_schema.h"

namespace terrier::plan_node {

class MaterializationPlanNode : public AbstractPlanNode {
 public:
  /**
   * Instantiate a MaterializationPlanNode
   * @param old_to_new_cols mapping from input to output columns
   * @param schema  output schema
   * @param physify_flag indicate whether to create a physical tile
   */
  explicit MaterializationPlanNode(std::shared_ptr<OutputSchema> output_schema, bool physify_flag)
      : AbstractPlanNode(output_schema), physify_flag_(physify_flag) {}

  /**
   * Instantiate a MaterializationPlanNode
   * @param physify_flag indicate whether to create a physical tile
   */
  explicit MaterializationPlanNode(bool physify_flag) : AbstractPlanNode(nullptr), physify_flag_(physify_flag) {}

  /**
   * @return physify flag
   */
  bool GetPhysifyFlag() const { return physify_flag_; }

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::MATERIALIZE; }

  DISALLOW_COPY_AND_MOVE(MaterializationPlanNode);

 private:
  /**
   * Whether to create a physical tile or just pass through underlying
   * logical tile
   */
  bool physify_flag_;
};

}  // namespace terrier::plan_node
