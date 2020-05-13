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
    double outer_rows = memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
    double inner_rows = memo_->GetGroupByID(gexpr_->GetChildGroupId(1))->GetNumRows();
    auto total_row_count = memo_->GetGroupByID(gexpr_->GetGroupID())->GetNumRows();

    // automatically set row counts to 1 if given counts aren't valid
    if (outer_rows <= 0) {
      outer_rows = 1;
    }

    if (inner_rows <= 0) {
      inner_rows = 1;
    }

    double rows = outer_rows; // set default cardinality for now

    // set cardinality based on type of nl join
    if (op->GetJoinType() == PhysicalJoinType::INNER) {
      rows = memo_->GetGroupByID(gexpr_->GetGroupID())->GetNumRows();
    }

    double num_tuples;
    double total_cpu_cost_per_tuple;

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
    total_cpu_cost_per_tuple = GetCPUCostForQuals(const_cast<std::vector<AnnotatedExpression> &&>(op->GetJoinPredicates())) + tuple_cpu_cost;

    // calculate total cpu cost for all tuples
    output_cost_ = num_tuples * total_cpu_cost_per_tuple + outer_rows + tuple_cpu_cost * total_row_count;
  }

  /**
   * Visit a InnerHashJoin operator
   * @param op operator
   */
  void Visit(UNUSED_ATTRIBUTE const InnerHashJoin *op) override {
    // get num rows for both tables that are being joined
    // left child cols should be inserted in the hash table, while right is hashed to check for equality
    double left_rows = memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
    double right_rows = memo_->GetGroupByID(gexpr_->GetChildGroupId(1))->GetNumRows();
    auto total_row_count = memo_->GetGroupByID(gexpr_->GetGroupID())->GetNumRows();

    auto left_table_name = op->GetLeftKeys()[0].CastManagedPointerTo<parser::ColumnValueExpression>()->GetTableName();

    double frac_null;
    double num_distinct;
    double avg_freq;

    // overall saved estimations
    auto bucket_size_frac = 1.0;
    auto mcv_freq = 1.0;

    for (const auto &pred : op->GetJoinPredicates()) {
      // current estimated stats on left table
      double curr_bucket_size_frac;
      double curr_mcv_freq;

      auto curr_left_child = pred.GetExpr()->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
      auto curr_right_child = pred.GetExpr()->GetChild(1).CastManagedPointerTo<parser::ColumnValueExpression>();
      auto curr_left_table_name = curr_left_child->GetTableName();
      auto curr_right_table_name = curr_right_child->GetTableName();

      common::ManagedPointer<ColumnStats> col_stats;
      if (curr_left_table_name == left_table_name) {
        col_stats = stats_storage_->GetTableStats(curr_left_child->GetDatabaseOid(), curr_left_child->GetTableOid())->GetColumnStats(curr_left_child->GetColumnOid());
      } else {
        col_stats = stats_storage_->GetTableStats(curr_right_child->GetDatabaseOid(), curr_right_child->GetTableOid())->GetColumnStats(curr_right_child->GetColumnOid());
      }

      // using the stats of the column referred to in the join predicate
      // estimate # of buckets for each ht: cardinality * 2 (mock real hash table which aims for load factor of 0.5)
      double buckets = col_stats->GetCardinality() * 2;
      curr_mcv_freq = col_stats->GetCommonFreqs()[0];
      num_distinct = col_stats->GetCardinality();
      frac_null = col_stats->GetFracNull();
      avg_freq = (1.0 - frac_null) / num_distinct;

      // get ratio of col rows with restrict clauses applied over all possible rows (w/o restrictions)
      auto overall_col_ratio = total_row_count / std::max(left_rows, right_rows);

      if (total_row_count > 0) {
        num_distinct *= overall_col_ratio;
        if (num_distinct < 1.0) {
          num_distinct = 1.0;
        } else {
          num_distinct = uint32_t(num_distinct);
        }
      }

      if (num_distinct > buckets) {
        curr_bucket_size_frac = 1.0 / buckets;
      } else {
        curr_bucket_size_frac = 1.0 / num_distinct;
      }

      if (avg_freq > 0.0 && curr_mcv_freq > avg_freq) {
        curr_bucket_size_frac *= curr_mcv_freq / avg_freq;
      }

      if (curr_bucket_size_frac < 1.0e-6) {
        curr_bucket_size_frac = 1.0e-6;
      } else if (curr_bucket_size_frac > 1.0) {
        curr_bucket_size_frac = 1.0;
      }

      if (bucket_size_frac > curr_bucket_size_frac) {
        bucket_size_frac = curr_bucket_size_frac;
      }

      if (mcv_freq > curr_mcv_freq) {
        mcv_freq = curr_mcv_freq;
      }
    }

    auto hash_cost = GetCPUCostForQuals(const_cast<std::vector<AnnotatedExpression> &&>(op->GetJoinPredicates()));

    auto row_est = right_rows * bucket_size_frac * 0.5;
    if (row_est < 1.0) {
      row_est = 1.0;
    } else {
      row_est = uint32_t(row_est);
    }

    output_cost_ = hash_cost * left_rows * row_est * 0.5 + tuple_cpu_cost * total_row_count;
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
  double GetCPUCostForQuals(std::vector<AnnotatedExpression> &&qualifiers) {
    auto total_cost = 1.f;
    for (const auto &q : qualifiers) {
      total_cost += GetCPUCostPerQual(q.GetExpr());
    }
    return total_cost;
  }

  /**
   * Calculates the CPU cost for one qualifier
   * @param qualifier - qualifer to calculate cost for
   * @return cost of qualifier
   */
  double GetCPUCostPerQual(common::ManagedPointer<parser::AbstractExpression> qualifier) {
    auto qual_type = qualifier->GetExpressionType();
    auto total_cost = 1.f;
    if (qual_type == parser::ExpressionType::FUNCTION) {
      //TODO(viv): find out how to calculate cost of function
    } else if (qual_type == parser::ExpressionType::OPERATOR_UNARY_MINUS || // not really proud of this ...
        qual_type == parser::ExpressionType::OPERATOR_PLUS ||
        qual_type == parser::ExpressionType::OPERATOR_MINUS ||
        qual_type == parser::ExpressionType::OPERATOR_MULTIPLY ||
        qual_type == parser::ExpressionType::OPERATOR_DIVIDE ||
        qual_type == parser::ExpressionType::OPERATOR_CONCAT ||
        qual_type == parser::ExpressionType::OPERATOR_MOD ||
        qual_type == parser::ExpressionType::OPERATOR_CAST ||
        qual_type == parser::ExpressionType::OPERATOR_IS_NULL ||
        qual_type == parser::ExpressionType::OPERATOR_IS_NOT_NULL ||
        qual_type == parser::ExpressionType::OPERATOR_EXISTS ||
        qual_type == parser::ExpressionType::OPERATOR_NULL_IF ||
        qual_type == parser::ExpressionType::COMPARE_EQUAL) {
      total_cost += op_cpu_cost;
    }
    for (const auto &c : qualifier->GetChildren()) {
      total_cost += GetCPUCostPerQual(c);
    }
    //TODO(viv): add more casing to cost other expr types
    return total_cost;
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
  double tuple_cpu_cost = 2.f;

  /**
   * Cost to execute an operator
   * TODO(viv): find a better constant for op cost (?)
   */
  double op_cpu_cost = 2.f;

  /**
   * Computed output cost
   */
  double output_cost_ = 0.f;
};

}  // namespace optimizer
