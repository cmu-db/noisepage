#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/output_schema.h"
#include "planner/plannodes/plan_visitor.h"

namespace noisepage::planner {

/**
 * Plan node for set operation:
 * INTERSECT/INTERSECT ALL/EXPECT/EXCEPT ALL
 *
 * @warning UNION (ALL) is handled differently.
 * IMPORTANT: Both children must have the same physical schema.
 */
class SetOpPlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for an delete plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param set_op set operation of this plan node
     * @return builder object
     */
    Builder &SetSetOp(SetOpType set_op) {
      set_op_ = set_op;
      return *this;
    }

    /**
     * Build the setop plan node
     * @return plan node
     */
    std::unique_ptr<SetOpPlanNode> Build() {
      return std::unique_ptr<SetOpPlanNode>(
          new SetOpPlanNode(std::move(children_), std::move(output_schema_), set_op_));
    }

   protected:
    /**
     * Set Operation of this node
     */
    SetOpType set_op_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param set_op the set pperation of this node
   */
  SetOpPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children, std::unique_ptr<OutputSchema> output_schema,
                SetOpType set_op)
      : AbstractPlanNode(std::move(children), std::move(output_schema)), set_op_(set_op) {}

 public:
  /**
   * Default constructor for deserialization
   */
  SetOpPlanNode() = default;

  DISALLOW_COPY_AND_MOVE(SetOpPlanNode)

  /**
   * @return the set operation of this node
   */
  SetOpType GetSetOp() const { return set_op_; }

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::SETOP; }

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
   * Set Operation of this node
   */
  SetOpType set_op_;
};

DEFINE_JSON_HEADER_DECLARATIONS(SetOpPlanNode);

}  // namespace noisepage::planner
