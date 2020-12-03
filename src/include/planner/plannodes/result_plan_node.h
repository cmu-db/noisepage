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
     * @param expr the expression used to derived the output tuple
     * @return builder object
     */
    Builder &SetExpr(common::ManagedPointer<parser::AbstractExpression> expr) {
      expr_ = expr;
      return *this;
    }

    /**
     * Build the setop plan node
     * @return plan node
     */
    std::unique_ptr<ResultPlanNode> Build() {
      return std::unique_ptr<ResultPlanNode>(
          new ResultPlanNode(std::move(children_), std::move(output_schema_), expr_, plan_node_id_));
    }

   protected:
    /**
     * The expression used to derived the output tuple
     */
    common::ManagedPointer<parser::AbstractExpression> expr_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param tuple the tuple in the storage layer
   * @param plan_node_id Plan node id
   */
  ResultPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children, std::unique_ptr<OutputSchema> output_schema,
                 common::ManagedPointer<parser::AbstractExpression> expr, plan_node_id_t plan_node_id)
      : AbstractPlanNode(std::move(children), std::move(output_schema), plan_node_id), expr_(expr) {}

 public:
  /**
   * Default constructor used for deserialization
   */
  ResultPlanNode() = default;

  DISALLOW_COPY_AND_MOVE(ResultPlanNode)

  /** @return the tuple in the storage layer */
  common::ManagedPointer<parser::AbstractExpression> GetExpression() const { return common::ManagedPointer(expr_); }

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::RESULT; }

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
   * Expression used to derived the output tuple
   */
  common::ManagedPointer<parser::AbstractExpression> expr_;
};

DEFINE_JSON_HEADER_DECLARATIONS(ResultPlanNode);

}  // namespace noisepage::planner
