#pragma once

#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>
#include "common/hash_util.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression_defs.h"
#include "plan_node/abstract_plan_node.h"

// TODO(Gus, Wen): Replace Perform Binding in AggregateTerm and AggregatePlanNode
// TODO(Gus, Wen): Replace VisitParameters
// TODO(Gus, Wen): figure out global aggregates

namespace terrier::plan_node {

/**
 * Plan node for aggregates
 */
class AggregatePlanNode : public AbstractPlanNode {
 public:
  /**
   * Information for each term being aggregated on
   */
  class AggregateTerm {
   public:
    /**
     *
     * @param aggregate_type Aggregate expression type
     * @param expr pointer to aggregate expression
     * @param distinct distinct flag
     */
    AggregateTerm(parser::ExpressionType aggregate_type, parser::AbstractExpression *expr, bool distinct)
        : aggregate_type_(aggregate_type), expression_(expr), distinct_(distinct) {}

    /**
     * Count, Sum, Min, Max, etc
     */
    parser::ExpressionType aggregate_type_;
    /**
     * Aggregate expression
     */
    const parser::AbstractExpression *expression_;
    /**
     * Distinct flag for aggragate term (example COUNT(distinct order))
     */
    bool distinct_;
  };

 protected:
  /**
   * Builder for aggregate plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    /**
     * Dont allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param term aggregate term to be added
     * @return builder object
     */
    Builder &AddAgregateTerm(AggregateTerm term) {
      aggregate_terms_.push_back(term);
      return *this;
    }

    /**
     * @param predicate having clause predicate to use for aggregate term
     * @return builder object
     */
    Builder &SetHavingClausePredicate(std::unique_ptr<const parser::AbstractExpression> &&predicate) {
      having_clause_predicate_ = std::move(predicate);
      return *this;
    }

    /**
     * @param strategy aggregation strategy to be used
     * @return builder object
     */
    Builder &SetAggregateStrategyType(AggregateStrategyType strategy) {
      aggregate_strategy_ = strategy;
      return *this;
    }

    /**
     * Build the aggregate plan node
     * @return plan node
     */
    std::shared_ptr<AggregatePlanNode> Build() {
      return std::shared_ptr<AggregatePlanNode>(
          new AggregatePlanNode(std::move(children_), std::move(output_schema_), estimated_cardinality_,
                                std::move(having_clause_predicate_), std::move(aggregate_terms_), aggregate_strategy_));
    }

   protected:
    /**
     * Predicate for having clause if it exists
     */
    std::unique_ptr<const parser::AbstractExpression> having_clause_predicate_;
    /**
     * List of aggregate terms for aggregation
     */
    std::vector<AggregateTerm> aggregate_terms_;
    /**
     * Strategy to use for aggregation
     */
    AggregateStrategyType aggregate_strategy_;
  };

  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param estimated_cardinality estimated cardinality of output of node
   * @param having_clause_predicate unique pointer to possible having clause predicate
   * @param aggregate_terms vector of aggregate terms for the aggregation
   * @param aggregate_strategy aggregation strategy to be used
   */
  AggregatePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                    std::shared_ptr<OutputSchema> output_schema, uint32_t estimated_cardinality,
                    std::unique_ptr<const parser::AbstractExpression> &&having_clause_predicate,
                    std::vector<AggregateTerm> aggregate_terms, AggregateStrategyType aggregate_strategy)
      : AbstractPlanNode(std::move(children), std::move(output_schema), estimated_cardinality),
        having_clause_predicate_(std::move(having_clause_predicate)),
        aggregate_terms_(std::move(aggregate_terms)),
        aggregate_strategy_(aggregate_strategy) {}

 public:
  ~AggregatePlanNode() override {
    for (auto term : aggregate_terms_) {
      delete term.expression_;
    }
  }

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  // TODO(Gus,Wen): Figure out how to represent global aggregates (example: count(*))
  /**
   * A global aggregate does not aggregate over specific columns, example: count(*)
   * @return true if aggregation is global
   */
  bool IsGlobal() const { return false; }

  /**
   * @return pointer to predicate for having clause
   */
  const std::unique_ptr<const parser::AbstractExpression> &GetHavingClausePredicate() const {
    return having_clause_predicate_;
  }

  /**
   * @return vector of aggregate terms
   */
  const std::vector<AggregateTerm> &GetAggregateTerms() const { return aggregate_terms_; }

  /**
   * @return aggregation strategy
   */
  AggregateStrategyType GetAggregateStrategyType() const { return aggregate_strategy_; }

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::AGGREGATE; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

 private:
  /**
   * @return true of two vectors of aggregate terms are equal
   */
  bool AreEqual(const std::vector<AggregatePlanNode::AggregateTerm> &A,
                const std::vector<AggregatePlanNode::AggregateTerm> &B) const;

  /**
   * @param agg_terms aggregate terms to be hashed
   * @return hash of agregate terms
   */
  common::hash_t HashAggregateTerms(const std::vector<AggregatePlanNode::AggregateTerm> &agg_terms) const;

 private:
  std::unique_ptr<const parser::AbstractExpression> having_clause_predicate_;
  const std::vector<AggregateTerm> aggregate_terms_;
  const AggregateStrategyType aggregate_strategy_;

 public:
  /**
   * Dont allow plan to be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(AggregatePlanNode);
};

}  // namespace terrier::plan_node
