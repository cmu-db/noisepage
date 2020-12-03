#pragma once

#include "common/macros.h"
#include "optimizer/cost_model/abstract_cost_model.h"

namespace noisepage::optimizer {

class Memo;
class GroupExpression;

/**
 * This cost model is meant to just be a trivial cost model. The decisions it makes are as follows
 * Always choose index scan (cost of 0) over sequential scan (cost of 1)
 * Choose NL if left rows is a single record (for single record lookup queries), else choose hash join
 * Choose hash group by over sort group by
 */
class TrivialCostModel : public AbstractCostModel {
 public:
  /**
   * Cost of performing a scan
   * Meant as a rough heuristic to ensure that INDEX_SCANs are always picked
   */
  static constexpr double SCAN_COST = 1000000.f;

  /**
   * Cost of performing a NLJoin
   */
  static constexpr double NLJOIN_COST = 1000000.f;

  /**
   * Default constructor
   */
  TrivialCostModel() = default;

  /**
   * Costs a GroupExpression
   * @param txn TransactionContext that query is generated under
   * @param accessor CatalogAccessor
   * @param memo Memo object containing all relevant groups
   * @param gexpr GroupExpression to calculate cost for
   */
  double CalculateCost(transaction::TransactionContext *txn, catalog::CatalogAccessor *accessor, Memo *memo,
                       GroupExpression *gexpr) override;

  /**
   * Visit a SeqScan operator
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const SeqScan *op) override { output_cost_ = SCAN_COST; }

  /**
   * Visit a IndexScan operator
   * @param op operator
   */
  void Visit(const IndexScan *op) override;

  /**
   * Visit a QueryDerivedScan operator
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const QueryDerivedScan *op) override { output_cost_ = 0.f; }

  /**
   * Visit a OrderBy operator
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const OrderBy *op) override { output_cost_ = 0.f; }

  /**
   * Visit a Limit operator
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const Limit *op) override { output_cost_ = 0.f; }

  /**
   * Visit a InnerIndexJoin operator
   * @param op operator
   */
  void Visit(const InnerIndexJoin *op) override;

  /**
   * Visit a InnerNLJoin operator
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const InnerNLJoin *op) override { output_cost_ = NLJOIN_COST; }

  /**
   * Visit a LeftNLJoin operator
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const LeftNLJoin *op) override {}

  /**
   * Visit a RightNLJoin operator
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const RightNLJoin *op) override {}

  /**
   * Visit a OuterNLJoin operator
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const OuterNLJoin *op) override {}

  /**
   * Visit a InnerHashJoin operator
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const InnerHashJoin *op) override { output_cost_ = NLJOIN_COST + 1.0f; }

  /**
   * Visit a LeftHashJoin operator
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const LeftHashJoin *op) override {}

  /**
   * Visit a RightHashJoin operator
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const RightHashJoin *op) override {}

  /**
   * Visit a OuterHashJoin operator
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const OuterHashJoin *op) override {}

  /**
   * Visit a LeftSemiHashJoin operator
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const LeftSemiHashJoin *op) override { output_cost_ = 1.f; }

  /**
   * Visit a Insert operator
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const Insert *op) override {}

  /**
   * Visit a InsertSelect operator
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const InsertSelect *op) override {}

  /**
   * Visit a Delete operator
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const Delete *op) override {}

  /**
   * Visit a Update operator
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const Update *op) override {}

  /**
   * Visit a HashGroupBy operator
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const HashGroupBy *op) override { output_cost_ = 0.f; }

  /**
   * Visit a SortGroupBy operator
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const SortGroupBy *op) override { output_cost_ = 1.f; }

  /**
   * Visit a Aggregate operator
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const Aggregate *op) override { output_cost_ = 0.f; }

 private:
  /**
   * GroupExpression to cost
   */
  GroupExpression *gexpr_;

  /**
   * Memo table to use
   */
  Memo *memo_;

  /**
   * Transaction Context
   */
  transaction::TransactionContext *txn_;

  /**
   * Accessor
   */
  catalog::CatalogAccessor *accessor_;

  /**
   * Computed output cost
   */
  double output_cost_ = 0;
};

}  // namespace noisepage::optimizer
