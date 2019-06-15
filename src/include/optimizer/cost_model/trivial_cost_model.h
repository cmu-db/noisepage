#pragma once

#include "optimizer/cost_model/abstract_cost_model.h"
#include "optimizer/group_expression.h"
#include "optimizer/physical_operators.h"
#include "parser/expression/tuple_value_expression.h"
#include "transaction/transaction_context.h"

// This cost model is meant to just be a trivial cost model. The decisions it makes are as follows
// * Always choose index scan (cost of 0) over sequential scan (cost of 1)
// * Choose NL if left rows is a single record (for single record lookup queries), else choose hash join
// * Choose hash group by over sort group by

namespace terrier {
namespace optimizer {

class Memo;
class GroupExpression;

class TrivialCostModel : public AbstractCostModel {
 public:
  TrivialCostModel() = default;

  double CalculateCost(GroupExpression *gexpr, Memo *memo, transaction::TransactionContext *txn) override {
    gexpr_ = gexpr;
    memo_ = memo;
    txn_ = txn;
    gexpr_->Op().Accept(this);
    return output_cost_;
  };

  void Visit(UNUSED_ATTRIBUTE const SeqScan *op) override { output_cost_ = 1.f; }

  void Visit(UNUSED_ATTRIBUTE const IndexScan *op) override { output_cost_ = 0.f; }

  void Visit(UNUSED_ATTRIBUTE const QueryDerivedScan *op) override { output_cost_ = 0.f; }

  void Visit(UNUSED_ATTRIBUTE const OrderBy *) override { output_cost_ = 0.f; }

  void Visit(UNUSED_ATTRIBUTE const Limit *op) override { output_cost_ = 0.f; }

  void Visit(UNUSED_ATTRIBUTE const InnerNLJoin *op) override { output_cost_ = 0.f; }

  void Visit(UNUSED_ATTRIBUTE const LeftNLJoin *op) override {}
  void Visit(UNUSED_ATTRIBUTE const RightNLJoin *op) override {}
  void Visit(UNUSED_ATTRIBUTE const OuterNLJoin *op) override {}

  void Visit(UNUSED_ATTRIBUTE const InnerHashJoin *op) override { output_cost_ = 1.f; }

  void Visit(UNUSED_ATTRIBUTE const LeftHashJoin *op) override {}
  void Visit(UNUSED_ATTRIBUTE const RightHashJoin *op) override {}
  void Visit(UNUSED_ATTRIBUTE const OuterHashJoin *op) override {}
  void Visit(UNUSED_ATTRIBUTE const Insert *op) override {}
  void Visit(UNUSED_ATTRIBUTE const InsertSelect *op) override {}
  void Visit(UNUSED_ATTRIBUTE const Delete *op) override {}
  void Visit(UNUSED_ATTRIBUTE const Update *op) override {}

  void Visit(UNUSED_ATTRIBUTE const HashGroupBy *op) override { output_cost_ = 0.f; }
  void Visit(UNUSED_ATTRIBUTE const SortGroupBy *op) override { output_cost_ = 1.f; }
  void Visit(UNUSED_ATTRIBUTE const Distinct *op) override { output_cost_ = 0.f; }
  void Visit(UNUSED_ATTRIBUTE const Aggregate *op) override { output_cost_ = 0.f; }

 private:
  GroupExpression *gexpr_;
  Memo *memo_;
  transaction::TransactionContext *txn_;
  double output_cost_ = 0;
};

}  // namespace optimizer
}  // namespace terrier
