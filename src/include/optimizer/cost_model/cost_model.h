#pragma once

#include "optimizer/cost_model/abstract_cost_model.h"
#include "optimizer/memo.h"
#include "optimizer/group_expression.h"
#include "optimizer/physical_operators.h"
#include "optimizer/statistics/stats_storage.h"
#include "optimizer/statistics/table_stats.h"
#include "transaction/transaction_context.h"

namespace terrier::optimizer {

/**
 * Cost model based on the PostgreSQL cost model formulas.
 */
class CostModel : public AbstractCostModel {
 public:
  /**
   * Default constructor
   */
  CostModel() = default;

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
  void Visit(UNUSED_ATTRIBUTE const SeqScan *op) override {
    auto table_stats = stats_storage_->GetTableStats(op->GetDatabaseOID(), op->GetTableOID());
    if (table_stats->GetColumnCount() == 0) {
      output_cost_ = 1.f;
      return;
    }
    output_cost_ = table_stats->GetNumRows() * tuple_cpu_cost;
  }

  /**
   * Visit a IndexScan operator
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const IndexScan *op) override {
    auto table_stats = stats_storage_->GetTableStats(op->GetDatabaseOID(), op->GetTableOID());
    if (table_stats->GetColumnCount() == 0 || table_stats->GetNumRows() == 0) {
      output_cost_ = 0.f;
      return;
    }
    output_cost_ = std::log2(table_stats->GetNumRows()) * tuple_cpu_cost +
        memo_->GetGroupByID(gexpr_->GetGroupID())->GetNumRows() * tuple_cpu_cost;
  }

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
  void Visit(UNUSED_ATTRIBUTE const Limit *op) override {
    auto child_num_rows = memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
    output_cost_ =
        std::min(static_cast<size_t>(child_num_rows), static_cast<size_t>(op->GetLimit())) * tuple_cpu_cost;
  }

  /**
   * Visit a NLJoin operator
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const NLJoin *op) override {
    // this is the overall cost algorithm for now; will add initial cost optimization later
    double num_tuples;
    double outer_rows = memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
    double inner_rows = memo_->GetGroupByID(gexpr_->GetChildGroupId(1))->GetNumRows();
    double total_cpu_cost_per_tuple;

    // automatically set row counts to 1 if given counts aren't valid
    if (outer_rows <= 0) {
      outer_rows = 1;
    }

    if (inner_rows <= 0) {
      inner_rows = 1;
    }

    // semi-joins are specially cased on due to their implementation - in many cases, the scan will
    // stop early if a match is found.
    if (op->GetJoinType() == PhysicalJoinType::SEMI) {
      //TODO(viv): write the cost est. for this. Will leave blank for now since the calculation itself is quite complex
      // and this is for one special case.
      num_tuples = outer_rows * inner_rows;
    } else { // all other cases are computed by simply considering all tuple pairs
      num_tuples = outer_rows * inner_rows;
    }

    // compute cpu cost per tuple
    // formula: cpu cost for evaluating all qualifier clauses for the join per tuple + cpu cost to emit tuple
    total_cpu_cost_per_tuple = GetCPUCostPerQual(const_cast<std::vector<AnnotatedExpression> &&>(op->GetJoinPredicates())) + tuple_cpu_cost;

    //TODO(viv): add estimation of # of output rows and calculate cost to materialize each row

    // calculate total cpu cost for all tuples
    output_cost_ = num_tuples * total_cpu_cost_per_tuple;
  }

  /**
   * Visit a InnerHashJoin operator
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const InnerHashJoin *op) override { output_cost_ = 1.f; }

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

  /**
   * Sets stats storage variable
   * @param storage StatsStorage object
   */
  void SetStatsStorage(StatsStorage *storage) { stats_storage_ = storage; }

 private:
  /**
   * Calculates the CPU cost (for one tuple) to evaluate all qualifiers
   * @param qualifiers - list of qualifiers to be evaluated
   * @return CPU cost
   */
  double GetCPUCostPerQual(std::vector<AnnotatedExpression> &&qualifiers) {
    return 0.f;
  }

  /**
   * Statistics storage object for all tables
   */
  StatsStorage *stats_storage_;

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
  double tuple_cpu_cost = 0.15;

  /**
   * Computed output cost
   */
  double output_cost_ = 0.f;
};

}  // namespace optimizer
