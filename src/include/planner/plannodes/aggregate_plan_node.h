#pragma once

#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "common/hash_util.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/aggregate_expression.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/plan_visitor.h"

// TODO(Gus, Wen): Replace Perform Binding in parser::AggregateExpression* and AggregatePlanNode
// TODO(Gus, Wen): Replace VisitParameters
// TODO(Gus, Wen): figure out global aggregates

namespace noisepage::planner {

using GroupByTerm = common::ManagedPointer<parser::AbstractExpression>;
using AggregateTerm = common::ManagedPointer<parser::AggregateExpression>;
using GroupByTerm = common::ManagedPointer<parser::AbstractExpression>;

/**
 * Plan node for aggregates
 */
class AggregatePlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for aggregate plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param term aggregate term to be added
     * @return builder object
     */
    Builder &AddAggregateTerm(AggregateTerm term) {
      aggregate_terms_.emplace_back(term);
      return *this;
    }

    /**
     * @param term groupby term to be added
     * @return builder object
     */
    Builder &AddGroupByTerm(GroupByTerm term) {
      groupby_terms_.emplace_back(term);
      return *this;
    }

    /**
     * @param predicate having clause predicate to use for aggregate term
     * @return builder object
     */
    Builder &SetHavingClausePredicate(common::ManagedPointer<parser::AbstractExpression> predicate) {
      having_clause_predicate_ = predicate;
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
    std::unique_ptr<AggregatePlanNode> Build();

   protected:
    /**
     * Group By Expressions
     */
    std::vector<GroupByTerm> groupby_terms_;
    /**
     * Predicate for having clause if it exists
     */
    common::ManagedPointer<parser::AbstractExpression> having_clause_predicate_;
    /**
     * List of aggregate terms for aggregation
     */
    std::vector<AggregateTerm> aggregate_terms_;
    /**
     * Strategy to use for aggregation
     */
    AggregateStrategyType aggregate_strategy_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param groupby_terms Group By terms
   * @param having_clause_predicate unique pointer to possible having clause predicate
   * @param aggregate_terms vector of aggregate terms for the aggregation
   * @param aggregate_strategy aggregation strategy to be used
   */
  AggregatePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                    std::unique_ptr<OutputSchema> output_schema, std::vector<GroupByTerm> groupby_terms,
                    common::ManagedPointer<parser::AbstractExpression> having_clause_predicate,
                    std::vector<AggregateTerm> aggregate_terms, AggregateStrategyType aggregate_strategy);

 public:
  /**
   * Default constructor used for deserialization
   */
  AggregatePlanNode() = default;

  DISALLOW_COPY_AND_MOVE(AggregatePlanNode)

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  /**
   * @return vector of group by terms
   */
  const std::vector<GroupByTerm> &GetGroupByTerms() const { return groupby_terms_; }

  /**
   * @return pointer to predicate for having clause
   */
  common::ManagedPointer<parser::AbstractExpression> GetHavingClausePredicate() const {
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

  /** @return True if this plan node can be executed with a static aggregation. */
  bool IsStaticAggregation() const { return GetGroupByTerms().empty(); }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  void Accept(common::ManagedPointer<PlanVisitor> v) const override { v->Visit(this); }

  bool operator==(const AbstractPlanNode &rhs) const override;

  nlohmann::json ToJson() const override;
  std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
  std::vector<GroupByTerm> groupby_terms_;
  common::ManagedPointer<parser::AbstractExpression> having_clause_predicate_;
  std::vector<AggregateTerm> aggregate_terms_;
  AggregateStrategyType aggregate_strategy_;
};
DEFINE_JSON_HEADER_DECLARATIONS(AggregatePlanNode);
}  // namespace noisepage::planner
