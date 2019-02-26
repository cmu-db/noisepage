#pragma once

#include "abstract_plan_node.h"
#include "catalog/schema.h"

// TODO(Gus,Wen): I don't think limit really needs an output schema. I'll include it for now, but we can maybe toss it

namespace terrier::plan_node {

class LimitPlanNode : public AbstractPlanNode {
 public:
  LimitPlanNode(catalog::Schema output_schema, size_t limit, size_t offset)
      : AbstractPlanNode(output_schema), limit_(limit), offset_(offset) {}

  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::LIMIT; }

  size_t GetLimit() { return limit_; }

  size_t GetOffset() { return offset_; }

  std::unique_ptr<AbstractPlanNode> Copy() const override {
    return std::unique_ptr<AbstractPlanNode>(new LimitPlanNode(GetOutputSchema(), limit_, offset_));
  }

  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override { return !(*this == rhs); }

 private:
  // The limit
  const size_t limit_;

  // The offset
  const size_t offset_;

  DISALLOW_COPY_AND_MOVE(LimitPlanNode);
};
}  // namespace terrier::plan_node
