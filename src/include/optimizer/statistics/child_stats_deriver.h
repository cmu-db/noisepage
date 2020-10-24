#pragma once

#include <vector>

#include "optimizer/group_expression.h"
#include "optimizer/operator_visitor.h"

namespace noisepage::parser {
class AbstractExpression;
}  // namespace noisepage::parser

namespace noisepage::optimizer {

class Memo;

/**
 * Derive child stats that have not yet been calculated for
 * a logical group expression.
 */
class ChildStatsDeriver : public OperatorVisitor {
 public:
  /**
   * Derives child statistics for an input logical group expression
   * @param gexpr Logical Group Expression to derive for
   * @param required_cols Expressions that derived stats must include
   * @param memo Memo table
   * @returns Set indicating what columns require stats
   */
  std::vector<ExprSet> DeriveInputStats(GroupExpression *gexpr, ExprSet required_cols, Memo *memo);

  /**
   * Visit for a LogicalQueryDerivedGet
   * @param op Visiting LogicalQueryDerivedGet
   */
  void Visit(const LogicalQueryDerivedGet *op) override;

  /**
   * Visit for a LogicalInnerJoin
   * @param op Visiting LogicalInnerJoin
   */
  void Visit(const LogicalInnerJoin *op) override;

  /**
   * Visit for a LogicalLeftJoin
   * @param op Visiting LogicalLeftJoin
   */
  void Visit(const LogicalLeftJoin *op) override;

  /**
   * Visit for a LogicalRightJoin
   * @param op Visiting LogicalRightJoin
   */
  void Visit(const LogicalRightJoin *op) override;

  /**
   * Visit for a LogicalOuterJoin
   * @param op Visiting LogicalOuterJoin
   */
  void Visit(const LogicalOuterJoin *op) override;

  /**
   * Visit for a LogicalSemiJoin
   * @param op Visiting LogicalSemiJoin
   */
  void Visit(const LogicalSemiJoin *op) override;

  /**
   * Visit for a LogicalAggregateAndGroupBy
   * @param op Visiting LogicalAggregateAndGroupBy
   */
  void Visit(const LogicalAggregateAndGroupBy *op) override;

 private:
  /**
   * Function to pass down all required_cols_ to output list
   */
  void PassDownRequiredCols();

  /**
   * Function for passing down a single column
   * @param col Column to passdown
   */
  void PassDownColumn(common::ManagedPointer<parser::AbstractExpression> col);

  /**
   * Set of required child stats columns
   */
  ExprSet required_cols_;

  /**
   * GroupExpression to derive for
   */
  GroupExpression *gexpr_;

  /**
   * Memo
   */
  Memo *memo_;

  std::vector<ExprSet> output_;
};

}  // namespace noisepage::optimizer
