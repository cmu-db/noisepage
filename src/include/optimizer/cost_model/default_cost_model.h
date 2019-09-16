#pragma once

#include "optimizer/cost_model/abstract_cost_model.h"
#include "optimizer/memo.h"
#include "optimizer/physical_operators.h"
#include "optimizer/statistics/stats_storage.h"
#include "optimizer/statistics/table_stats.h"

namespace terrier::optimizer {

class Memo;
// Derive cost for a physical group expression
class DefaultCostModel : public AbstractCostModel {
 public:
  DefaultCostModel() = default;

  double CalculateCost(GroupExpression *gexpr, Memo *memo, transaction::TransactionContext *txn) override {
    gexpr_ = gexpr;
    memo_ = memo;
    txn_ = txn;
    gexpr_->Op().Accept(this);
    return output_cost_;
  }

  void Visit(const SeqScan *op) override {
    auto table_stats = stats_storage_->GetTableStats(op->GetDatabaseOID(), op->GetTableOID());
    if (table_stats->GetColumnCount() == 0) {
      output_cost_ = 1.f;
      return;
    }
    output_cost_ = table_stats->GetNumRows() * DEFAULT_TUPLE_COST;
  }
  void Visit(UNUSED_ATTRIBUTE const IndexScan *op) override {
    auto table_stats = stats_storage_->GetTableStats(op->GetDatabaseOID(), op->GetTableOID());
    if (table_stats->GetColumnCount() == 0 || table_stats->GetNumRows() == 0) {
      output_cost_ = 0.f;
      return;
    }
    output_cost_ = std::log2(table_stats->GetNumRows()) * DEFAULT_INDEX_TUPLE_COST +
                   memo_->GetGroupByID(gexpr_->GetGroupID())->GetNumRows() * DEFAULT_TUPLE_COST;
  }

  void Visit(UNUSED_ATTRIBUTE const QueryDerivedScan *op) override { output_cost_ = 0.f; }

  void Visit(const OrderBy *) override { SortCost(); }

  void Visit(const Limit *op) override {
    auto child_num_rows = memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();

    output_cost_ = std::min((size_t)child_num_rows, (size_t)op->GetLimit()) * DEFAULT_TUPLE_COST;
  }
  void Visit(UNUSED_ATTRIBUTE const InnerNLJoin *op) override {
    auto left_child_rows = memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
    auto right_child_rows = memo_->GetGroupByID(gexpr_->GetChildGroupId(1))->GetNumRows();

    output_cost_ = left_child_rows * right_child_rows * DEFAULT_TUPLE_COST;
  }
  void Visit(UNUSED_ATTRIBUTE const LeftNLJoin *op) override {}
  void Visit(UNUSED_ATTRIBUTE const RightNLJoin *op) override {}
  void Visit(UNUSED_ATTRIBUTE const OuterNLJoin *op) override {}
  void Visit(UNUSED_ATTRIBUTE const InnerHashJoin *op) override {
    auto left_child_rows = memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
    auto right_child_rows = memo_->GetGroupByID(gexpr_->GetChildGroupId(1))->GetNumRows();
    output_cost_ = (left_child_rows + right_child_rows) * DEFAULT_TUPLE_COST;
  }
  void Visit(UNUSED_ATTRIBUTE const LeftHashJoin *op) override {}
  void Visit(UNUSED_ATTRIBUTE const RightHashJoin *op) override {}
  void Visit(UNUSED_ATTRIBUTE const OuterHashJoin *op) override {}
  void Visit(UNUSED_ATTRIBUTE const Insert *op) override {}
  void Visit(UNUSED_ATTRIBUTE const InsertSelect *op) override {}
  void Visit(UNUSED_ATTRIBUTE const Delete *op) override {}
  void Visit(UNUSED_ATTRIBUTE const Update *op) override {}
  void Visit(UNUSED_ATTRIBUTE const HashGroupBy *op) override { output_cost_ = HashCost() + GroupByCost(); }
  void Visit(UNUSED_ATTRIBUTE const SortGroupBy *op) override { output_cost_ = GroupByCost(); }
  void Visit(UNUSED_ATTRIBUTE const Distinct *op) override { output_cost_ = HashCost(); }
  void Visit(UNUSED_ATTRIBUTE const Aggregate *op) override { output_cost_ = HashCost() + GroupByCost(); }

 private:
  double HashCost() {
    auto child_num_rows = memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
    return child_num_rows * DEFAULT_TUPLE_COST;
  }

  double SortCost() {
    auto child_num_rows = memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
    if (child_num_rows == 0) {
      return 1.0f;
    }
    return child_num_rows * std::log2(child_num_rows) * DEFAULT_TUPLE_COST;
  }

  double GroupByCost() {
    auto child_num_rows = memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
    return child_num_rows * DEFAULT_TUPLE_COST;
  }

  StatsStorage *stats_storage_;
  GroupExpression *gexpr_;
  Memo *memo_;
  transaction::TransactionContext *txn_;
  double output_cost_ = 0;
};

}  // namespace terrier::optimizer
