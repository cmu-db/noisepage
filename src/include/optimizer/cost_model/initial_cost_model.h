#pragma once

#include "optimizer/cost_model/abstract_cost_model.h"
#include "optimizer/group_expression.h"
#include "optimizer/physical_operators.h"
#include "transaction/transaction_context.h"

namespace terrier::optimizer {

// This cost model calculates the lower bound cost of the given GroupExpression
class InitialCostModel : public AbstractCostModel {
 public:
  /**
   * Default constructor
   */
  InitialCostModel() = default;

  /**
   * Costs a GroupExpression
   * @param txn TransactionContext that query is generated under
   * @param memo Memo object containing all relevant groups
   * @param gexpr GroupExpression to calculate cost for
   */
  double CalculateCost(transaction::TransactionContext *txn, Memo *memo, GroupExpression *gexpr) override {
    gexpr_ = gexpr;
    memo_ = memo;
    txn_ = txn;
    gexpr_->Op().Accept(common::ManagedPointer<OperatorVisitor>(this));
    return output_cost_;
  };

  /**
   * Visit a SeqScan operator
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const SeqScan *op) override { output_cost_ = 1.f; }

  /**
   * Visit a IndexScan operator
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const IndexScan *op) override { output_cost_ = 0.f; }

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
   * Visit a NLJoin operator.
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const NLJoin *op) override {
    double outer_rows = memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
    double inner_rows = memo_->GetGroupByID(gexpr_->GetChildGroupId(1))->GetNumRows();

    double total_cost = 0.0;

    // computes cost of scanning the entire inner rel per outer row
    if (outer_rows > 1) {
      total_cost += outer_rows * memo_->GetGroupByID(gexpr_->GetChildGroupId(1))->GetCostLB();
    }

    output_cost_ = total_cost;
  }

  /**
   * Visit a InnerHashJoin operator
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const InnerHashJoin *op) override {
    double outer_rows = memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
    double inner_rows = memo_->GetGroupByID(gexpr_->GetChildGroupId(1))->GetNumRows();

    double total_cost = 0.0;

    total_cost += memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetCostLB();
    total_cost += (op_cpu_cost * op->GetJoinPredicates().size() + tuple_cpu_cost) * inner_rows;
    total_cost += op_cpu_cost * op->GetJoinPredicates().size() * outer_rows;

    output_cost_ = total_cost;
  }

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
   * CPU cost to materialize a tuple
   * TODO(viv): change later to be evaluated per instantiation via a benchmark
   */
  double tuple_cpu_cost = 2.f;

  /**
   * Cost to execute an operator
   * TODO(viv): find a better constant for op cost (?)
   */
  double op_cpu_cost = 2.f;

  /**
   * Computed output cost
   */
  double output_cost_ = 0;
};

}
