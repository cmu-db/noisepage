#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "planner/plannodes/abstract_join_plan_node.h"
#include "planner/plannodes/plan_visitor.h"

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
    Builder &SetLeftKeys(std::vector<common::ManagedPointer<parser::AbstractExpression>> left_keys) {
      left_keys_ = std::move(left_keys);
      return *this;
    }

    /**
     * Set right keys
     * @param right_keys Right keys
     * @returns builder object
     */
    Builder &SetRightKeys(std::vector<common::ManagedPointer<parser::AbstractExpression>> right_keys) {
      right_keys_ = std::move(right_keys);
      return *this;
    }

    /**
     * Build the nested loop join plan node
     * @return plan node
     */
    std::unique_ptr<NestedLoopJoinPlanNode> Build() {
      return std::unique_ptr<NestedLoopJoinPlanNode>(new NestedLoopJoinPlanNode(
          std::move(children_), std::move(output_schema_), join_type_, join_predicate_, left_keys_, right_keys_));
    }

   protected:
    /**
     * Left keys
     */
    std::vector<common::ManagedPointer<parser::AbstractExpression>> left_keys_;

    /**
     * Right keys
     */
    std::vector<common::ManagedPointer<parser::AbstractExpression>> right_keys_;
  };

 private:
  /**
   * @param children child plan nodes, first child is inner loop, second child is outer loop
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param join_type logical join type
   * @param predicate join predicate
   * @param left_keys Left keys (derived from child 0) to join on
   * @param right_keys Right keys (derived from child 1) to join on
   */
  NestedLoopJoinPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                         std::unique_ptr<OutputSchema> output_schema, LogicalJoinType join_type,
                         common::ManagedPointer<parser::AbstractExpression> predicate,
                         std::vector<common::ManagedPointer<parser::AbstractExpression>> left_keys,
                         std::vector<common::ManagedPointer<parser::AbstractExpression>> right_keys)
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
  const std::vector<common::ManagedPointer<parser::AbstractExpression>> &GetLeftKeys() const { return left_keys_; }

  /**
   * @return right keys
   */
  const std::vector<common::ManagedPointer<parser::AbstractExpression>> &GetRightKeys() const { return right_keys_; }

  /**
   * @return the NestedLooped value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  void Accept(common::ManagedPointer<PlanVisitor> v) const override { v->Visit(this); }

  nlohmann::json ToJson() const override;
  std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
  /**
   * Left keys (derived from child 0) to join on
   */
  std::vector<common::ManagedPointer<parser::AbstractExpression>> left_keys_;

  /**
   * Right keys (derived from child 1) to join on
   */
  std::vector<common::ManagedPointer<parser::AbstractExpression>> right_keys_;
};

DEFINE_JSON_DECLARATIONS(NestedLoopJoinPlanNode);

}  // namespace terrier::planner
