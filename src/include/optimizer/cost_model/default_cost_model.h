#pragma once

#include "optimizer/cost_model/abstract_cost_model.h"
#include "expression/tuple_value_expression.h"
#include "catalog/table_catalog.h"
#include "optimizer/memo.h"
#include "optimizer/physical_operators.h"
#include "optimizer/statistics/stats_storage.h"
#include "optimizer/statistics/table_stats.h"

namespace terrier::optimizer {

class Memo;
// Derive cost for a physical group expression
class DefaultCostModel : public AbstractCostModel {
 public:
  DefaultCostModel(){};

  double CalculateCost(GroupExpression *gexpr, Memo *memo,
                       concurrency::TransactionContext *txn) {
    gexpr_ = gexpr;
    memo_ = memo;
    txn_ = txn;
    gexpr_->Op().Accept(this);
    return output_cost_;
  }

  void Visit(UNUSED_ATTRIBUTE const DummyScan *op) {
    output_cost_ = 0.f;
  }
  void Visit(const SeqScan *op) {
    auto table_stats = std::dynamic_pointer_cast<TableStats>(
        StatsStorage::GetInstance()->GetTableStats(
            op->table_->GetDatabaseOid(), op->table_->GetTableOid(), txn_));
    if (table_stats->GetColumnCount() == 0) {
      output_cost_ = 1.f;
      return;
    }
    output_cost_ = table_stats->num_rows * DEFAULT_TUPLE_COST;
  }
  void Visit(UNUSED_ATTRIBUTE const IndexScan *op) {
    auto table_stats = std::dynamic_pointer_cast<TableStats>(
        StatsStorage::GetInstance()->GetTableStats(
            op->table_->GetDatabaseOid(), op->table_->GetTableOid(), txn_));
    if (table_stats->GetColumnCount() == 0 || table_stats->num_rows == 0) {
      output_cost_ = 0.f;
      return;
    }
    // Index search cost + scan cost
    output_cost_ = std::log2(table_stats->num_rows) * DEFAULT_INDEX_TUPLE_COST +
        memo_->GetGroupByID(gexpr_->GetGroupID())->GetNumRows() *
            DEFAULT_TUPLE_COST;
  }

  void Visit(UNUSED_ATTRIBUTE const QueryDerivedScan *op) {
    output_cost_ = 0.f;
  }

  void Visit(const OrderBy *) { SortCost(); }

  void Visit(const Limit *op) {
    auto child_num_rows =
        memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();

    output_cost_ =
        std::min((size_t)child_num_rows, (size_t)op->limit) * DEFAULT_TUPLE_COST;
  }
  void Visit(UNUSED_ATTRIBUTE const InnerNLJoin *op) {
    auto left_child_rows =
        memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
    auto right_child_rows =
        memo_->GetGroupByID(gexpr_->GetChildGroupId(1))->GetNumRows();

    output_cost_ = left_child_rows * right_child_rows * DEFAULT_TUPLE_COST;
  }
  void Visit(UNUSED_ATTRIBUTE const LeftNLJoin *op) {}
  void Visit(UNUSED_ATTRIBUTE const RightNLJoin *op) {}
  void Visit(UNUSED_ATTRIBUTE const OuterNLJoin *op) {}
  void Visit(UNUSED_ATTRIBUTE const InnerHashJoin *op) {
    auto left_child_rows =
        memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
    auto right_child_rows =
        memo_->GetGroupByID(gexpr_->GetChildGroupId(1))->GetNumRows();
    // TODO(boweic): Build (left) table should have different cost to probe table
    output_cost_ = (left_child_rows + right_child_rows) * DEFAULT_TUPLE_COST;
  }
  void Visit(UNUSED_ATTRIBUTE const LeftHashJoin *op) {}
  void Visit(UNUSED_ATTRIBUTE const RightHashJoin *op) {}
  void Visit(UNUSED_ATTRIBUTE const OuterHashJoin *op) {}
  void Visit(UNUSED_ATTRIBUTE const Insert *op) {}
  void Visit(UNUSED_ATTRIBUTE const InsertSelect *op) {}
  void Visit(UNUSED_ATTRIBUTE const Delete *op) {}
  void Visit(UNUSED_ATTRIBUTE const Update *op) {}
  void Visit(UNUSED_ATTRIBUTE const HashGroupBy *op) {
    // TODO(boweic): Integrate hash in groupby may cause us to miss the
    // opportunity to further optimize some query where the child output is
    // already hashed by the GroupBy key, we'll do a hash anyway
    output_cost_ = HashCost() + GroupByCost();
  }
  void Visit(UNUSED_ATTRIBUTE const SortGroupBy *op) {
    // Sort group by does not sort the tuples, it requires input columns to be
    // sorted
    output_cost_ = GroupByCost();
  }
  void Visit(UNUSED_ATTRIBUTE const Distinct *op) {
    output_cost_ = HashCost();
  }
  void Visit(UNUSED_ATTRIBUTE const Aggregate *op) {
    // TODO(boweic): Ditto, separate groupby operator and implementation(e.g.
    // hash, sort) may enable opportunity for further optimization
    output_cost_ = HashCost() + GroupByCost();
  }

 private:

  double HashCost() {
    auto child_num_rows =
        memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
    // O(tuple)
    return child_num_rows * DEFAULT_TUPLE_COST;
  }

  double SortCost() {
    auto child_num_rows =
        memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();

    if (child_num_rows == 0) {
      return 1.0f;
    }
    // O(tuple * log(tuple))
    return child_num_rows * std::log2(child_num_rows) * DEFAULT_TUPLE_COST;
  }

  double GroupByCost() {
    auto child_num_rows =
        memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
    // O(tuple)
    return child_num_rows * DEFAULT_TUPLE_COST;
  }

  GroupExpression *gexpr_;
  Memo *memo_;
  concurrency::TransactionContext *txn_;
  double output_cost_ = 0;
};

}  // namespace terrier::optimizer
