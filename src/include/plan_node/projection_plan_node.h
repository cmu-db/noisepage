#pragma once

#include <memory>
#include <string>
#include <utility>
#include "plan_node/abstract_plan_node.h"

namespace terrier::plan_node {

/**
 * Plan node for projection
 */
class ProjectionPlanNode : public AbstractPlanNode {
 public:
  /**
   * @param output_schema Schema representing the structure of the output of this plan node
   */
  explicit ProjectionPlanNode(std::shared_ptr<OutputSchema> output_schema)
      : AbstractPlanNode(std::move(output_schema)) {}

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::PROJECTION; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override { return !(*this == rhs); }

 public:
  DISALLOW_COPY_AND_MOVE(ProjectionPlanNode);
};

}  // namespace terrier::plan_node
