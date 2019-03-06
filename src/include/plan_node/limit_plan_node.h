#pragma once

#include "abstract_plan_node.h"
#include "catalog/schema.h"

// TODO(Gus,Wen): I don't think limit really needs an output schema. I'll include it for now, but we can maybe toss it

namespace terrier::plan_node {

class LimitPlanNode : public AbstractPlanNode {
 public:
  LimitPlanNode(std::shared_ptr<catalog::Schema> output_schema, size_t limit, size_t offset)
      : AbstractPlanNode(output_schema), limit_(limit), offset_(offset) {}

  // For Deserialization
  LimitPlanNode() {}

  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::LIMIT; }

  size_t GetLimit() { return limit_; }

  size_t GetOffset() { return offset_; }

  std::unique_ptr<AbstractPlanNode> Copy() const override {
    return std::unique_ptr<AbstractPlanNode>(new LimitPlanNode(GetOutputSchema(), limit_, offset_));
  }

  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override { return !(*this == rhs); }

  nlohmann::json ToJson() const override;
  void FromJson(const nlohmann::json &json) override;

 private:
  // The limit
  size_t limit_;

  // The offset
  size_t offset_;

  DISALLOW_COPY_AND_MOVE(LimitPlanNode);
};

DEFINE_JSON_DECLARATIONS(LimitPlanNode);

}  // namespace terrier::plan_node
