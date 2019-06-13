#pragma once

#include "optimizer/memo.h"
#include "optimizer/physical_operators.h"
#include "optimizer/cost_model/abstract_cost_model.h"

// This cost model is meant to just be a trivial cost model. The decisions it makes are as follows
// * Always choose index scan (cost of 0) over sequential scan (cost of 1)
// * Choose NL if left rows is a single record (for single record lookup queries), else choose hash join
// * Choose hash group by over sort group by

namespace terrier {
namespace optimizer {

class TrivialCostModel : public AbstractCostModel {
 public:
  /**
   * Trivial constructor
   */
  TrivialCostModel(){};

  /**
   * Costs a GroupExpression
   * @param gexpr GroupExpression to calculate cost for
   * @param memo Memo object containing all relevant groups
   * @param txn TransactionContext that query is generated under
   */
  double CalculateCost(GroupExpression *gexpr, Memo *memo,
                       transaction::TransactionContext *txn) override {
    gexpr_ = gexpr;
    memo_ = memo;
    txn_ = txn;
    gexpr_->Op().Accept(this);
    return output_cost_;
  };

  void Visit(UNUSED_ATTRIBUTE const TableFreeScan *op) override {
    output_cost_ = 0.f;
  }

  void Visit(UNUSED_ATTRIBUTE const SeqScan *op) override {
    output_cost_ = 1.f;
  }

  void Visit(UNUSED_ATTRIBUTE const IndexScan *op) override {
    output_cost_ = 0.f;
  }

  void Visit(UNUSED_ATTRIBUTE const QueryDerivedScan *op) override {
    output_cost_ = 0.f;
  }

  void Visit(UNUSED_ATTRIBUTE const OrderBy *) override {
    output_cost_ = 0.f;
  }

  void Visit(UNUSED_ATTRIBUTE const Limit *op) override {
    output_cost_ = 0.f;
  }

  void Visit(UNUSED_ATTRIBUTE const InnerNLJoin *op) override {
    auto left_child_rows =
        memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
    if (left_child_rows == 1) {
      output_cost_ = 0.f;
    } else {
      output_cost_ = 2.f;
    }
  }

  void Visit(UNUSED_ATTRIBUTE const LeftNLJoin *op) override {}
  void Visit(UNUSED_ATTRIBUTE const RightNLJoin *op) override {}
  void Visit(UNUSED_ATTRIBUTE const OuterNLJoin *op) override {}

  void Visit(UNUSED_ATTRIBUTE const InnerHashJoin *op) override {
    output_cost_ = 1.f;
  }

  void Visit(UNUSED_ATTRIBUTE const LeftHashJoin *op) override {}
  void Visit(UNUSED_ATTRIBUTE const RightHashJoin *op) override {}
  void Visit(UNUSED_ATTRIBUTE const OuterHashJoin *op) override {}
  void Visit(UNUSED_ATTRIBUTE const Insert *op) override {}
  void Visit(UNUSED_ATTRIBUTE const InsertSelect *op) override {}
  void Visit(UNUSED_ATTRIBUTE const Delete *op) override{}
  void Visit(UNUSED_ATTRIBUTE const Update *op) override {}

  void Visit(UNUSED_ATTRIBUTE const HashGroupBy *op) override {
    output_cost_ = 0.f;
  }
  void Visit(UNUSED_ATTRIBUTE const SortGroupBy *op) override {
    output_cost_ = 1.f;
  }
  void Visit(UNUSED_ATTRIBUTE const Distinct *op) override {
    output_cost_ = 0.f;
  }
  void Visit(UNUSED_ATTRIBUTE const Aggregate *op) override {
    output_cost_ = 0.f;
  }

 private:
  GroupExpression *gexpr_;
  Memo *memo_;
  transaction::TransactionContext *txn_;
  double output_cost_ = 0;
};

}  // namespace optimizer
}  // namespace terrier
