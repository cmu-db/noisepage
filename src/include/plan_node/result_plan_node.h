#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "plan_node/abstract_plan_node.h"
#include "plan_node/output_schema.h"
#include "storage/storage_defs.h"

// TODO(Gus,Wen) Tuple as a concept does not exist yet, someone need to define it in the storage layer, possibly a
// collection of TransientValues
namespace terrier::plan_node {

/**
 * Result plan node.
 * The counter-part of Postgres Result plan
 * that returns a single constant tuple.
 */
class ResultPlanNode : public AbstractPlanNode {
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
     * @param tuple the tuple in the storage layer
     * @return builder object
     */
    Builder &SetTuple(std::shared_ptr<parser::AbstractExpression> expr) {
      expr_ = std::move(expr);
      return *this;
    }

    /**
     * Build the setop plan node
     * @return plan node
     */
    std::shared_ptr<ResultPlanNode> Build() {
      return std::shared_ptr<ResultPlanNode>(
          new ResultPlanNode(std::move(children_), std::move(output_schema_), std::move(expr_)));
    }

   protected:
    /**
     * The expression used to derived the output tuple
     */
    std::shared_ptr<parser::AbstractExpression> expr_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param tuple the tuple in the storage layer
   */
  ResultPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children, std::shared_ptr<OutputSchema> output_schema,
                 std::shared_ptr<parser::AbstractExpression> expr)
      : AbstractPlanNode(std::move(children), std::move(output_schema)), expr_(std::move(expr)) {}

 public:
  /**
   * @return the tuple in the storage layer
   */
  const std::shared_ptr<parser::AbstractExpression> GetExpression() const { return expr_; }

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::RESULT; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

 private:
  /**
   * the expression used to derived the output tuple
   */
  std::shared_ptr<parser::AbstractExpression> expr_;

 public:
  /**
   * Don't allow plan to be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(ResultPlanNode);
};

}  // namespace terrier::plan_node
