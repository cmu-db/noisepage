#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_join_plan_node.h"
#include "output_schema.h"
#include "parser/expression/abstract_expression.h"
#include "plan_node_defs.h"

namespace terrier::plan_node {

class MergeJoinPlanNode : public AbstractJoinPlanNode {
 public:
  struct JoinClause {
    explicit JoinClause(const parser::AbstractExpression *left, const parser::AbstractExpression *right, bool reversed)
        : left_(left), right_(right), reversed_(reversed) {}

    JoinClause(const JoinClause &other) = delete;

    /**
     * Move constructor
     * @param other the JoinClause to move
     */
    JoinClause(JoinClause &&other) noexcept
        : left_(std::move(other.left_)), right_(std::move(other.right_)), reversed_(other.reversed_) {}

    std::unique_ptr<const parser::AbstractExpression> left_;
    std::unique_ptr<const parser::AbstractExpression> right_;
    bool reversed_;
  };

  /**
   * Instantiate a MergeJoinPlanNode
   * @param output_schema the output schema of this plan node
   * @param join_type the type of join to perform
   * @param predicate the condition for join
   * @param join_clauses the join clauses
   */
  explicit MergeJoinPlanNode(std::shared_ptr<OutputSchema> output_schema, LogicalJoinType join_type,
                             parser::AbstractExpression *predicate, std::vector<JoinClause> join_clauses)
      : AbstractJoinPlanNode(std::move(output_schema), join_type, predicate),
        join_clauses_(std::move(join_clauses)) {}

  /**
   * @return the type of this plan node
   */
  inline PlanNodeType GetPlanNodeType() const override { return PlanNodeType::MERGEJOIN; }

  /**
   * @return join clauses
   */
  const std::vector<JoinClause> &GetJoinClauses() const { return join_clauses_; }

  /**
   * @return debug info
   */
  const std::string GetInfo() const override { return "MergeJoinPlan"; }

  /**
   * @return a unique pointer to a copy of this plan node
   */
  std::unique_ptr<AbstractPlanNode> Copy() const override;

 private:
  // the SQL join clauses
  std::vector<JoinClause> join_clauses_;

 private:
  DISALLOW_COPY_AND_MOVE(MergeJoinPlanNode);
};

}  // namespace terrier::plan_node