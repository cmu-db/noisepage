#pragma once

#include <memory>
#include <string>
#include <utility>
#include "catalog/schema.h"
#include "plan_node/abstract_plan_node.h"

// TODO(Gus,Wen): I don't think limit really needs an output schema. I'll include it for now, but we can maybe toss it

namespace terrier::plan_node {

/**
 * Plan node for a limit operator
 */
class LimitPlanNode : public AbstractPlanNode {
 protected:
  /**
   * Builder for limit plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param limit number of tuples to limit to
     * @return object
     */
    Builder &SetLimit(size_t limit) {
      limit_ = limit;
      return *this;
    }

    /**
     * @param offset offset for where to limit from
     * @return builder object
     */
    Builder &SetOffset(size_t offset) {
      offset_ = offset;
      return *this;
    }

    /**
     * Build the limit plan node
     * @return plan node
     */
    std::shared_ptr<LimitPlanNode> Build() {
      return std::shared_ptr<LimitPlanNode>(
          new LimitPlanNode(std::move(children_), std::move(output_schema_), estimated_cardinality_, limit_, offset_));
    }

   protected:
    size_t limit_;
    size_t offset_;
  };

  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param estimated_cardinality estimated cardinality of output of node
   * @param limit number of tuples to limit to
   * @param offset offset at which to limit from
   */
  LimitPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children, std::shared_ptr<OutputSchema> output_schema,
                uint32_t estimated_cardinality, size_t limit, size_t offset)
      : AbstractPlanNode(std::move(children), std::move(output_schema), estimated_cardinality),
        limit_(limit),
        offset_(offset) {}

  /**
   * Constructor used for JSON serialization
   */
  LimitPlanNode() = default;

 public:
  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::LIMIT; }

  /**
   * @return number to limit to
   */
  size_t GetLimit() { return limit_; }

  /**
   * @return offset for where to limit from
   */
  size_t GetOffset() { return offset_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override { return !(*this == rhs); }

  //  nlohmann::json ToJson() const override;
  //  void FromJson(const nlohmann::json &json) override;

 private:
  // The limit
  size_t limit_;

  // The offset
  size_t offset_;

 public:
  DISALLOW_COPY_AND_MOVE(LimitPlanNode);
};
}  // namespace terrier::plan_node
