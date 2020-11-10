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
   * @param required_cols Required column statistics
   * @param context OptimizerContext
   */
  void CalculateStats(GroupExpression *gexpr, ExprSet required_cols, OptimizerContext *context);

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
   * Add the base table stats if the base table maintain stats, or else
   * use default stats
   * @param col The column we want to get stats
   * @param table_stats Base table stats
   * @param stats The stats map to add
   */
  void AddBaseTableStats(common::ManagedPointer<parser::AbstractExpression> col,
                         common::ManagedPointer<TableStats> table_stats,
                         std::unordered_map<std::string, std::unique_ptr<ColumnStats>> *stats);

  /**
   * Return estimated cardinality for a filter
   * @param num_rows Number of rows of base table
   * @param predicate_stats The stats for columns in the expression
   * @param predicates conjunction predicates
   * @returns Estimated cardinality
   */
  size_t EstimateCardinalityForFilter(
      size_t num_rows, const std::unordered_map<std::string, std::unique_ptr<ColumnStats>> &predicate_stats,
      const std::vector<AnnotatedExpression> &predicates);

  /**
   * Calculates selectivity for predicate
   * @param predicate_table_stats Table Statistics
   * @param expr Predicate
   * @returns selectivity estimate
   */
  double CalculateSelectivityForPredicate(common::ManagedPointer<TableStats> predicate_table_stats,
                                          common::ManagedPointer<parser::AbstractExpression> expr);

  /**
   * Creates default ColumnStats
   * @param col ColumnValueExpression
   */
  std::unique_ptr<ColumnStats> CreateDefaultStats(common::ManagedPointer<parser::ColumnValueExpression> tv_expr) {
    return std::make_unique<ColumnStats>(tv_expr->GetDatabaseOid(), tv_expr->GetTableOid(), tv_expr->GetColumnOid(), 0,
                                         0.F, false, std::vector<double>{}, std::vector<double>{},
                                         std::vector<double>{}, true);
  }

  /**
   * GroupExpression
   */
  GroupExpression *gexpr_;

  /**
   * Required column statistics to generate
   */
  ExprSet required_cols_;

  /**
   * Metadata
   */
  OptimizerContext *context_;
};

}  // namespace noisepage::optimizer
