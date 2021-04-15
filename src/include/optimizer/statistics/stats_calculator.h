#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "optimizer/group_expression.h"
#include "optimizer/memo.h"
#include "optimizer/operator_visitor.h"
#include "optimizer/statistics/table_stats.h"
#include "parser/expression/column_value_expression.h"
#include "transaction/transaction_context.h"

namespace noisepage::optimizer {

/**
 * Derive stats for the root group using a group expression's children's stats
 */
class StatsCalculator : public OperatorVisitor {
 public:
  /**
   * Calculates stats for a logical GroupExpression
   * @param gexpr GroupExpression
   * @param context OptimizerContext
   */
  void CalculateStats(GroupExpression *gexpr, OptimizerContext *context);

  /**
   * Visit a LogicalGet
   * @param op Operator being visited
   */
  void Visit(const LogicalGet *op) override;

  /**
   * Visit a LogicalQueryDerivedGet
   * @param op Operator being visited
   */
  void Visit(const LogicalQueryDerivedGet *op) override;

  /**
   * Visit a LogicalInnerJoin
   * @param op Operator being visited
   */
  void Visit(const LogicalInnerJoin *op) override;

  /**
   * Visit a LogicalSemiJoin
   * @param op Operator being visited
   */
  void Visit(const LogicalSemiJoin *op) override;

  /**
   * Visit a LogicalAggregateAndGroupBy
   * @param op Operator being visited
   */
  void Visit(const LogicalAggregateAndGroupBy *op) override;

  /**
   * Visit a LogicalLimit
   * @param op Operator being visited
   */
  void Visit(const LogicalLimit *op) override;

 private:
  /**
   * Return estimated cardinality for a filter
   * @param num_rows Number of rows of base table
   * @param predicate_stats The stats for columns in the expression
   * @param predicates conjunction predicates
   * @returns Estimated cardinality
   */
  size_t EstimateCardinalityForFilter(size_t num_rows, const TableStats &predicate_stats,
                                      const std::vector<AnnotatedExpression> &predicates);

  /**
   * Calculates selectivity for predicate
   * @param predicate_table_stats Table Statistics
   * @param expr Predicate
   * @returns selectivity estimate
   */
  double CalculateSelectivityForPredicate(const TableStats &predicate_table_stats,
                                          common::ManagedPointer<parser::AbstractExpression> expr);

  /**
   * GroupExpression
   */
  GroupExpression *gexpr_;

  /**
   * Metadata
   */
  OptimizerContext *context_;
};

}  // namespace noisepage::optimizer
