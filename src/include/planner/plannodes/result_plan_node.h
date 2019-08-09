#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/output_schema.h"
#include "storage/storage_defs.h"

// TODO(Gus,Wen) Tuple as a concept does not exist yet, someone need to define it in the storage layer, possibly a
// collection of TransientValues
namespace terrier::planner {

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
    Builder &SetExpr(std::shared_ptr<parser::AbstractExpression> expr) {
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
  ResultPlanNode(std::vector<std::shared_ptr<AbstractPlanNode>> &&children, std::shared_ptr<OutputSchema> output_schema,
                 std::shared_ptr<parser::AbstractExpression> expr)
      : AbstractPlanNode(std::move(children), std::move(output_schema)), expr_(std::move(expr)) {}

 public:
  /**
   * Default constructor used for deserialization
   */
  ResultPlanNode() = default;

  DISALLOW_COPY_AND_MOVE(ResultPlanNode)

  /**
   * @return the tuple in the storage layer
   */
  std::shared_ptr<parser::AbstractExpression> GetExpression() const { return expr_; }

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::RESULT; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  nlohmann::json ToJson() const override;
  void FromJson(const nlohmann::json &j) override;

 private:
  /**
   * Expression used to derived the output tuple
   */
  std::shared_ptr<parser::AbstractExpression> expr_;
};

DEFINE_JSON_DECLARATIONS(ResultPlanNode);

}  // namespace terrier::planner
