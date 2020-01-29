#pragma once

#include <algorithm>

#include "optimizer/cost_model/abstract_cost_model.h"
#include "optimizer/memo.h"
#include "optimizer/physical_operators.h"
#include "optimizer/statistics/stats_storage.h"
#include "optimizer/statistics/table_stats.h"

namespace terrier::optimizer {

/**
 * The default cost model does more specific computation
 * for most of the operators to select a plan. Each calculation
 * takes the internal statistics of the specified table and
 * will output a cost based on the formula for that operator.
 * Some operators also require a hash/sort/group by cost.
 */
class DefaultCostModel : public AbstractCostModel {
 public:
  /**
   * Default constructor
   */
  DefaultCostModel() = default;

  /**
   * Takes a group expression and the memo table it belongs to and
   * calculates its cost
   * @param gexpr Group expression used to calculate cost
   * @param memo Memo table to track relevant group expressions
   * @param txn TransactionContext of query
   * @return calculated cost
   */
  double CalculateCost(transaction::TransactionContext *txn, Memo *memo, GroupExpression *gexpr) override {
    gexpr_ = gexpr;
    memo_ = memo;
    txn_ = txn;
    gexpr_->Op().Accept(this);
    return output_cost_;
  }

  /**
   * Seq scan operator to visit
   * @param op operator
   */
  void Visit(const SeqScan *op) override {
    auto table_stats = stats_storage_->GetTableStats(op->GetDatabaseOID(), op->GetTableOID());
    if (table_stats->GetColumnCount() == 0) {
      output_cost_ = DEFAULT_OUTPUT_COST;
      return;
    }
    output_cost_ = table_stats->GetNumRows() * DEFAULT_TUPLE_COST;
  }

  /**
   * Index scan operator to visit. The cost is calculated by approximating the number of index
   * entries based on the number of rows in the table and using the cost to access each index
   * entry to calculate the total cost of accessing all the entries. This cost is also
   * compounded by the general cost of accessing all of the tuples in the table.
   * @param op operator
   */
  void Visit(const IndexScan *op) override {
    auto table_stats = stats_storage_->GetTableStats(op->GetDatabaseOID(), op->GetTableOID());
    if (table_stats->GetColumnCount() == 0 || table_stats->GetNumRows() == 0) {
      output_cost_ = 0.f;
      return;
    }
    output_cost_ = std::log2(table_stats->GetNumRows()) * DEFAULT_INDEX_TUPLE_COST +
                   memo_->GetGroupByID(gexpr_->GetGroupID())->GetNumRows() * DEFAULT_TUPLE_COST;
  }

  /**
   * Query derived scan to visit
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const QueryDerivedScan *op) override { output_cost_ = 0.f; }

  /**
   * Order by operator to visit
   * @param op operator
   */
  void Visit(const OrderBy *op) override { SortCost(); }

  /**
   * Limit operator to visit
   * @param op operator
   */
  void Visit(const Limit *op) override {
    auto child_num_rows = memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();

    output_cost_ = std::min((size_t)child_num_rows, (size_t)op->GetLimit()) * DEFAULT_TUPLE_COST;
  }

  /**
   * InnerNL join operator to visit
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const InnerNLJoin *op) override {
    auto left_child_rows = memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
    auto right_child_rows = memo_->GetGroupByID(gexpr_->GetChildGroupId(1))->GetNumRows();

    output_cost_ = left_child_rows * right_child_rows * DEFAULT_TUPLE_COST;
  }

  /**
   * LeftNL join operator to visit
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const LeftNLJoin *op) override {}

  /**
   * RightNL join operator to visit
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const RightNLJoin *op) override {}

  /**
   * OuterNL join operator to visit
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const OuterNLJoin *op) override {}

  /**
   * Inner hash join operator to visit
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const InnerHashJoin *op) override {
    auto left_child_rows = memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
    auto right_child_rows = memo_->GetGroupByID(gexpr_->GetChildGroupId(1))->GetNumRows();
    output_cost_ = (left_child_rows + right_child_rows) * DEFAULT_TUPLE_COST;
  }

  /**
   * Left hash join operator to visit
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const LeftHashJoin *op) override {}

  /**
   * Right hash join operator to visit
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const RightHashJoin *op) override {}

  /**
   * Outer hash join operator to visit
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const OuterHashJoin *op) override {}

  /**
   * Insert operator to visit
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const Insert *op) override {}

  /**
   * Insert select operator to visit
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const InsertSelect *op) override {}

  /**
   * Delete operator to visit
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const Delete *op) override {}

  /**
   * Update operator to visit
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const Update *op) override {}

  /**
   * Hash group by operator to visit
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const HashGroupBy *op) override { output_cost_ = HashCost() + GroupByCost(); }

  /**
   * Sort group by operator to visit
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const SortGroupBy *op) override { output_cost_ = GroupByCost(); }

  /**
   * Aggregate operator to visit
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const Aggregate *op) override { output_cost_ = HashCost() + GroupByCost(); }

 private:
  /**
   * Function used to calculate cost of hashing based on the number of rows in the child group
   * @return the cost to hash
   */
  double HashCost() {
    auto child_num_rows = memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
    return child_num_rows * DEFAULT_TUPLE_COST;
  }

  /**
   * Function used to calculate cost of sorting based on number of rows in the child group
   * @return the cost to sort
   */
  double SortCost() {
    auto child_num_rows = memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
    if (child_num_rows == 0) {
      return 1.0f;
    }
    return child_num_rows * std::log2(child_num_rows) * DEFAULT_TUPLE_COST;
  }

  /**
   * Function used to calculate cost of grouping by based on number of rows in the child group
   * @return the cost to group by
   */
  double GroupByCost() {
    auto child_num_rows = memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
    return child_num_rows * DEFAULT_TUPLE_COST;
  }

  /**
   * Statistics storage object for all tables
   */
  StatsStorage *stats_storage_;

  /**
   * Group expression object to cost
   */
  GroupExpression *gexpr_;

  /**
   * Memo table
   */
  Memo *memo_;

  /**
   * Transaction context
   */
  transaction::TransactionContext *txn_;

  /**
   * Computed cost
   */
  double output_cost_ = 0;

  /**
   * Estimate the cost of processing each row during a query.
   */
  static constexpr double DEFAULT_TUPLE_COST = 0.01;

  /**
   * Estimate the cost of processing each index entry during an index scan.
   */
  static constexpr double DEFAULT_INDEX_TUPLE_COST = 0.005;

  /**
   * Default output cost if cost cannot be calculated.
   */
   static constexpr double DEFAULT_OUTPUT_COST = 1.0f;
};

}  // namespace terrier::optimizer
