#pragma once

#include <parser/expression/aggregate_expression.h>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>
#include "common/hash_util.h"
#include "parser/expression/abstract_expression.h"
#include "planner/plannodes/abstract_plan_node.h"

// TODO(Gus, Wen): Replace Perform Binding in AggregateTerm and AggregatePlanNode
// TODO(Gus, Wen): Replace VisitParameters
// TODO(Gus, Wen): figure out global aggregates

namespace terrier::planner {

using AggregateTerm = std::shared_ptr<parser::AggregateExpression>;
using GroupByTerm = std::shared_ptr<parser::AbstractExpression>;

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
      aggregate_terms_.emplace_back(std::move(term));
      return *this;
    }

    /**
     * @param predicate having clause predicate to use for aggregate term
     * @return builder object
     */
    Builder &SetHavingClausePredicate(std::shared_ptr<parser::AbstractExpression> predicate) {
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
     * @param term the term to add
     * @return builder object
     */
    Builder &AddGroupByTerm(GroupByTerm term) {
      group_by_terms_.emplace_back(term);
      return *this;
    }

    /**
     * Build the aggregate plan node
     * @return plan node
     */
    std::shared_ptr<AggregatePlanNode> Build() {
      return std::shared_ptr<AggregatePlanNode>(
          new AggregatePlanNode(std::move(children_), std::move(output_schema_), std::move(having_clause_predicate_),
                                std::move(aggregate_terms_), aggregate_strategy_, std::move(group_by_terms_)));
    }

   protected:
    /**
     * Predicate for having clause if it exists
     */
    std::shared_ptr<parser::AbstractExpression> having_clause_predicate_;
    /**
     * List of aggregate terms for aggregation
     */
    std::vector<AggregateTerm> aggregate_terms_;
    /**
     * Strategy to use for aggregation
     */
    AggregateStrategyType aggregate_strategy_;

    /**
     * List of group by terms
     */
    std::vector<GroupByTerm> group_by_terms_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param having_clause_predicate unique pointer to possible having clause predicate
   * @param aggregate_terms vector of aggregate terms for the aggregation
   * @param aggregate_strategy aggregation strategy to be used
   * @param group_by_terms vector of group by terms
   */
  AggregatePlanNode(std::vector<std::shared_ptr<AbstractPlanNode>> &&children,
                    std::shared_ptr<OutputSchema> output_schema,
                    std::shared_ptr<parser::AbstractExpression> having_clause_predicate,
                    std::vector<AggregateTerm> &&aggregate_terms, AggregateStrategyType aggregate_strategy,
                    std::vector<GroupByTerm> &&group_by_terms)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        having_clause_predicate_(std::move(having_clause_predicate)),
        aggregate_terms_(std::move(aggregate_terms)),
        aggregate_strategy_(aggregate_strategy),
        group_by_terms_(std::move(group_by_terms)) {}

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
   * @return pointer to predicate for having clause
   */
  const std::shared_ptr<parser::AbstractExpression> &GetHavingClausePredicate() const {
    return having_clause_predicate_;
  }

  /**
   * @return vector of aggregate terms
   */
  const std::vector<AggregateTerm> &GetAggregateTerms() const { return aggregate_terms_; }

  /**
   * @return vector of group by terms
   */
  const std::vector<GroupByTerm> &GetGroupByTerms() const { return group_by_terms_; }

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

  nlohmann::json ToJson() const override;
  void FromJson(const nlohmann::json &j) override;

 private:
  std::shared_ptr<parser::AbstractExpression> having_clause_predicate_;
  std::vector<AggregateTerm> aggregate_terms_;
  AggregateStrategyType aggregate_strategy_;
  std::vector<GroupByTerm> group_by_terms_;
};
DEFINE_JSON_DECLARATIONS(AggregatePlanNode);
}  // namespace terrier::planner
