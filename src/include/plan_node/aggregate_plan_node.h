#pragma once

#include <numeric>
#include "abstract_plan_node.h"
#include "common/hash_util.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression_defs.h"
#include "plan_node_defs.h"

// TODO(Gus, Wen): Replace Perform Binding in AggregateTerm and AggregatePlanNode
// TODO(Gus, Wen): Replace VisitParameters
// TODO(Gus, Wen): figure out global aggregates

namespace terrier::plan_node {

class AggregatePlanNode : public AbstractPlanNode {
 public:
  class AggregateTerm {
   public:
    parser::ExpressionType aggregate_type_;  // Count, Sum, Min, Max, etc
    const parser::AbstractExpression *expression_;
    bool distinct_;  // Distinct flag for aggragate term (example COUNT(distinct order))

    AggregateTerm(parser::ExpressionType aggregate_type, parser::AbstractExpression *expr, bool distinct = false)
        : aggregate_type_(aggregate_type), expression_(expr), distinct_(distinct) {}

    AggregateTerm Copy() const { return AggregateTerm(aggregate_type_, expression_->Copy().get(), distinct_); }
  };

  AggregatePlanNode(OutputSchema output_schema,
                    std::unique_ptr<const parser::AbstractExpression> &&having_clause_predicate,
                    const std::vector<AggregateTerm> &&aggregate_terms, AggregateStrategy aggregate_strategy)
      : AbstractPlanNode(output_schema),
        having_clause_predicate_(std::move(having_clause_predicate)),
        aggregate_terms_(aggregate_terms),
        aggregate_strategy_(aggregate_strategy) {}

  ~AggregatePlanNode() {
    for (auto term : aggregate_terms_) {
      delete term.expression_;
    }
  }

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  // TODO(Gus,Wen): Figure out how to represent global aggregates (example: count(*))
  bool IsGlobal() const { return false; }

  const parser::AbstractExpression *GetHavingClausePredicate() const { return having_clause_predicate_.get(); }

  const std::vector<AggregateTerm> &GetAggregateTerms() const { return aggregate_terms_; }

  AggregateStrategy GetAggregateStrategy() const { return aggregate_strategy_; }

  inline PlanNodeType GetPlanNodeType() const override { return PlanNodeType::AGGREGATE; }

  const std::string GetInfo() const { return "AggregatePlanNode"; }

  std::unique_ptr<AbstractPlanNode> Copy() const override;

  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override { return !(*this == rhs); }

 private:
  bool AreEqual(const std::vector<AggregatePlanNode::AggregateTerm> &A,
                const std::vector<AggregatePlanNode::AggregateTerm> &B) const;

  common::hash_t HashAggregateTerms(const std::vector<AggregatePlanNode::AggregateTerm> &agg_terms) const;

 private:
  /* For HAVING clause */
  std::unique_ptr<const parser::AbstractExpression> having_clause_predicate_;

  /* Aggregate terms */
  const std::vector<AggregateTerm> aggregate_terms_;

  /* Aggregate Strategy */
  const AggregateStrategy aggregate_strategy_;

 private:
  DISALLOW_COPY_AND_MOVE(AggregatePlanNode);
};

}  // namespace terrier::plan_node
