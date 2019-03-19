#pragma once

#include <memory>
#include <string>
#include <utility>
#include "plan_node/abstract_plan_node.h"

namespace terrier::plan_node {

class ProjectionPlanNode : public AbstractPlanNode {
 public:
  explicit ProjectionPlanNode(std::shared_ptr<OutputSchema> output_schema)
      : AbstractPlanNode(std::move(output_schema)) {}

  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::PROJECTION; }

  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override { return !(*this == rhs); }

 private:
  DISALLOW_COPY_AND_MOVE(ProjectionPlanNode);
};

}  // namespace terrier::plan_node
