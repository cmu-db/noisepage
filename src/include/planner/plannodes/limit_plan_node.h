#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/plan_visitor.h"

namespace noisepage::planner {

/**
 * Plan node for a limit operator
 */
class LimitPlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for limit plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
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
    std::unique_ptr<LimitPlanNode> Build() {
      return std::unique_ptr<LimitPlanNode>(
          new LimitPlanNode(std::move(children_), std::move(output_schema_), limit_, offset_));
    }

   protected:
    /**
     * Limit for plan
     */
    size_t limit_;
    /**
     * offset for plan
     */
    size_t offset_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param limit number of tuples to limit to
   * @param offset offset at which to limit from
   */
  LimitPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children, std::unique_ptr<OutputSchema> output_schema,
                size_t limit, size_t offset)
      : AbstractPlanNode(std::move(children), std::move(output_schema)), limit_(limit), offset_(offset) {}

 public:
  /**
   * Constructor used for JSON serialization
   */
  LimitPlanNode() = default;

  DISALLOW_COPY_AND_MOVE(LimitPlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::LIMIT; }

  /**
   * @return number to limit to
   */
  size_t GetLimit() const { return limit_; }

  /**
   * @return offset for where to limit from
   */
  size_t GetOffset() const { return offset_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  void Accept(common::ManagedPointer<PlanVisitor> v) const override { v->Visit(this); }

  nlohmann::json ToJson() const override;
  std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
  /**
   * The limit
   */
  size_t limit_;

  /**
   * The offset
   */
  size_t offset_;
};

DEFINE_JSON_HEADER_DECLARATIONS(LimitPlanNode);

}  // namespace noisepage::planner
