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

// TODO(Gus, Wen): Replace Perform Binding in parser::AggregateExpression* and AggregatePlanNode
// TODO(Gus, Wen): Replace VisitParameters
// TODO(Gus, Wen): figure out global aggregates

namespace terrier::planner {

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
    Builder &AddAggregateTerm(const parser::AggregateExpression *term) {
      aggregate_terms_.emplace_back(term);
      return *this;
    }

    /**
     * @param groupby_offsets
     * @return builder object
     */
    Builder &SetGroupByColOffsets(std::vector<unsigned> groupby_offsets) {
      groupby_offsets_ = std::move(groupby_offsets);
      return *this;
    }

    /**
     * @param predicate having clause predicate to use for aggregate term
     * @return builder object
     */
    Builder &SetHavingClausePredicate(const parser::AbstractExpression *predicate) {
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
    std::shared_ptr<AggregatePlanNode> Build() {
      return std::shared_ptr<AggregatePlanNode>(
          new AggregatePlanNode(std::move(children_), std::move(output_schema_), std::move(groupby_offsets_),
                                having_clause_predicate_, std::move(aggregate_terms_), aggregate_strategy_));
    }

   protected:
    /**
     * GroupBy column offsets
     * This will effectively let us support SELECT [AGGR] FROM [TBL] GROUP BY [COL],[COL]
     * TODO(wz2): Eventually support GROUP BY [expression]
     */
    std::vector<unsigned> groupby_offsets_;
    /**
     * Predicate for having clause if it exists
     */
    const parser::AbstractExpression *having_clause_predicate_;
    /**
     * List of aggregate terms for aggregation
     */
    std::vector<const parser::AggregateExpression *> aggregate_terms_;
    /**
     * Strategy to use for aggregation
     */
    AggregateStrategyType aggregate_strategy_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param having_clause_predicate unique pointer to possible having clause predicate
   * @param aggregate_terms vector of aggregate terms for the aggregation
   * @param aggregate_strategy aggregation strategy to be used
   */
  AggregatePlanNode(std::vector<std::shared_ptr<AbstractPlanNode>> &&children,
                    std::shared_ptr<OutputSchema> output_schema, std::vector<unsigned> groupby_offsets,
                    const parser::AbstractExpression *having_clause_predicate,
                    std::vector<const parser::AggregateExpression *> aggregate_terms,
                    AggregateStrategyType aggregate_strategy)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        groupby_offsets_(std::move(groupby_offsets)),
        having_clause_predicate_(having_clause_predicate),
        aggregate_terms_(std::move(aggregate_terms)),
        aggregate_strategy_(aggregate_strategy) {}

 public:
  /**
   * Default constructor used for deserialization
   */
  AggregatePlanNode() = default;

  ~AggregatePlanNode() override {
    for (auto *term : aggregate_terms_) {
      delete term;
    }
    delete having_clause_predicate_;
  }

  DISALLOW_COPY_AND_MOVE(AggregatePlanNode)

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  /**
   * Gets the offsets of columns to groupby
   */
  const std::vector<unsigned> &GetGroupByColumnOffsets() const { return groupby_offsets_; }

  /**
   * @return pointer to predicate for having clause
   */
  common::ManagedPointer<const parser::AbstractExpression> GetHavingClausePredicate() const {
    return common::ManagedPointer<const parser::AbstractExpression>(having_clause_predicate_);
  }

  /**
   * @return vector of aggregate terms
   */
  const std::vector<const parser::AggregateExpression *> &GetAggregateTerms() const { return aggregate_terms_; }

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
  /**
   * List of column offsets to group by.
   * These offsets are indexes into the tuple produced by the child
   */
  std::vector<unsigned> groupby_offsets_;
  const parser::AbstractExpression *having_clause_predicate_;
  std::vector<const parser::AggregateExpression *> aggregate_terms_;
  AggregateStrategyType aggregate_strategy_;
};
DEFINE_JSON_DECLARATIONS(AggregatePlanNode);
}  // namespace terrier::planner
