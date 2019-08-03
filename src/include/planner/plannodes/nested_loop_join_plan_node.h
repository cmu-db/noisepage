#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "planner/plannodes/abstract_join_plan_node.h"

namespace terrier::planner {

/**
 * Plan node for nested loop joins
 */
class NestedLoopJoinPlanNode : public AbstractJoinPlanNode {
 public:
  /**
   * Builder for nested loop join plan node
   */
  class Builder : public AbstractJoinPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * Set left keys
     * @param left_keys Left keys
     * @returns builder object
     */
    Builder &SetLeftKeys(std::vector<const parser::AbstractExpression *> left_keys) {
      left_keys_ = std::move(left_keys);
      return *this;
    }

    /**
     * Set right keys
     * @param right_keys Right keys
     * @returns builder object
     */
    Builder &SetRightKeys(std::vector<const parser::AbstractExpression *> right_keys) {
      right_keys_ = std::move(right_keys);
      return *this;
    }

    /**
     * Build the nested loop join plan node
     * @return plan node
     */
    std::shared_ptr<NestedLoopJoinPlanNode> Build() {
      return std::shared_ptr<NestedLoopJoinPlanNode>(new NestedLoopJoinPlanNode(
          std::move(children_), std::move(output_schema_), join_type_, join_predicate_, left_keys_, right_keys_));
    }

   protected:
    /**
     * Left keys
     */
    std::vector<const parser::AbstractExpression *> left_keys_;

    /**
     * Right keys
     */
    std::vector<const parser::AbstractExpression *> right_keys_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param join_type logical join type
   * @param predicate join predicate
   */
  NestedLoopJoinPlanNode(std::vector<std::shared_ptr<AbstractPlanNode>> &&children,
                         std::shared_ptr<OutputSchema> output_schema, LogicalJoinType join_type,
                         const parser::AbstractExpression *predicate,
                         std::vector<const parser::AbstractExpression *> left_keys,
                         std::vector<const parser::AbstractExpression *> right_keys)
      : AbstractJoinPlanNode(std::move(children), std::move(output_schema), join_type, predicate),
        left_keys_(std::move(left_keys)),
        right_keys_(std::move(right_keys)) {}

 public:
  /**
   * Default constructor used for deserialization
   */
  NestedLoopJoinPlanNode() = default;

  DISALLOW_COPY_AND_MOVE(NestedLoopJoinPlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::NESTLOOP; }

  /**
   * @return left keys
   */
  const std::vector<const parser::AbstractExpression *> &GetLeftKeys() const { return left_keys_; }

  /**
   * @return right keys
   */
  const std::vector<const parser::AbstractExpression *> &GetRightKeys() const { return right_keys_; }

  /**
   * @return the NestedLooped value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  nlohmann::json ToJson() const override;
  void FromJson(const nlohmann::json &j) override;

 private:
  /**
   * Left keys
   */
  std::vector<const parser::AbstractExpression *> left_keys_;

  /**
   * Right keys
   */
  std::vector<const parser::AbstractExpression *> right_keys_;
};

DEFINE_JSON_DECLARATIONS(NestedLoopJoinPlanNode);

}  // namespace terrier::planner
