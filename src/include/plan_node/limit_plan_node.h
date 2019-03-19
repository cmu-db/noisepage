#pragma once

#include <memory>
#include <string>
#include <utility>
#include "catalog/schema.h"
#include "plan_node/abstract_plan_node.h"

// TODO(Gus,Wen): I don't think limit really needs an output schema. I'll include it for now, but we can maybe toss it

namespace terrier::plan_node {

class LimitPlanNode : public AbstractPlanNode {
 public:
  LimitPlanNode(std::shared_ptr<OutputSchema> output_schema, size_t limit, size_t offset)
      : AbstractPlanNode(std::move(output_schema)), limit_(limit), offset_(offset) {}

  // For Deserialization
  LimitPlanNode() = default;

  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::LIMIT; }

  size_t GetLimit() { return limit_; }

  size_t GetOffset() { return offset_; }

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
