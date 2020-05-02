#include <utility>

#include "gtest/gtest.h"
#include "optimizer/cost_model/cost_model.h"
#include "optimizer/optimizer_context.h"
#include "optimizer/physical_operators.h"
#include "optimizer/statistics/histogram.h"
#include "optimizer/statistics/top_k_elements.h"
#include "optimizer/optimizer_defs.h"

#include "test_util/test_harness.h"

namespace terrier::optimizer {
class CostModelTests : public TerrierTest {
 protected:
  ColumnStats column_stats_obj_a_1_;
  ColumnStats column_stats_obj_b_1_;
  TableStats table_stats_obj_a_;
  TableStats table_stats_obj_b_;
  StatsStorage stats_storage_;
  CostModel cost_model_;

  static constexpr size_t NUM_ROWS_A = 100'000;
  static constexpr size_t NUM_ROWS_B = 5;

  void SetUp() override {
    // generate 100k random values and generate histogram for them.
    // This is used for A's "statistics", since it is unclear how to mimic histogram bounds "convincingly".
    /*
    Histogram<int> a_hist{100};
    std::default_random_engine generator;
    std::uniform_int_distribution<int> distribution(1, 100);
    for (int i = 0; i < NUM_ROWS_A; i++) {
      auto number = static_cast<int>(distribution(generator));
      a_hist.Increment(number);
    }
    std::vector<double> a_hist_bounds = a_hist.Uniform();
     */
    /*
    * @param database_id - database oid of column
    * @param table_id - table oid of column
    * @param column_id - column oid of column
     *
    * @param num_rows - number of rows in column
    * @param cardinality - cardinality of column
    * @param frac_null - fraction of null values out of total values in column
     *
    * @param most_common_vals - list of most common values in the column
    * @param most_common_freqs - list of the frequencies of the most common values in the column
    * @param histogram_bounds - the bounds of the histogram of the column e.g. (1.0 - 4.0)
        * @param is_base_table - indicates whether the column is from a base table
        */
    column_stats_obj_a_1_ = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(1),
                                      NUM_ROWS_A, NUM_ROWS_A / 2, 0.2, {1, 2, 3}, {5, 5, 5}, {1.0, 5.0}, true);
    column_stats_obj_b_1_ = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(2), catalog::col_oid_t(1),
                                      NUM_ROWS_B, NUM_ROWS_B, 0.0, {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
    table_stats_obj_a_ = TableStats(
        catalog::db_oid_t(1), catalog::table_oid_t(1), NUM_ROWS_A, true,
        {column_stats_obj_a_1_});
    table_stats_obj_b_ = TableStats(
        catalog::db_oid_t(1), catalog::table_oid_t(2), NUM_ROWS_B, true,
        {column_stats_obj_b_1_});
    stats_storage_ = StatsStorage();
    cost_model_ = CostModel();
    cost_model_.SetStatsStorage(&stats_storage_);
  }
};

// NOLINTNEXTLINE
TEST_F(CostModelTests, InnerNLJoinOrderTest) {
  stats_storage_.InsertTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1), std::move(table_stats_obj_a_));
  stats_storage_.InsertTableStats(catalog::db_oid_t(1), catalog::table_oid_t(2), std::move(table_stats_obj_b_));
  OptimizerContext optimizer_context =
    OptimizerContext(common::ManagedPointer<AbstractCostModel>(&cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto annotated_expr_1 = AnnotatedExpression(x_1, std::unordered_set<std::string>());
  Operator inner_nl_join_a_first = NLJoin::Make(PhysicalJoinType::INNER, std::vector<AnnotatedExpression>());
  Operator inner_nl_join_b_first = NLJoin::Make(PhysicalJoinType::INNER, std::vector<AnnotatedExpression>());
  // child operators: one scans the first table, while the other scans the 2nd table. The join will operate on these two
  // tables.
  Operator seq_scan_a = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), catalog::table_oid_t(1),
                                    std::vector<AnnotatedExpression>(), "table", false);
  Operator seq_scan_b = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), catalog::table_oid_t(2),
                                    std::vector<AnnotatedExpression>(), "table", false);
  std::vector<std::unique_ptr<OperatorNode>> children_a_first = {};
  children_a_first.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan_a, {})));
  children_a_first.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan_b, {})));

  std::vector<std::unique_ptr<OperatorNode>> children_b_first = {};
  children_b_first.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan_b, {})));
  children_b_first.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan_a, {})));

  OperatorNode operator_expression_a_first = OperatorNode(inner_nl_join_a_first, std::move(children_a_first));
  OperatorNode operator_expression_b_first = OperatorNode(inner_nl_join_b_first, std::move(children_b_first));
  GroupExpression *grexp_a_first =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&operator_expression_a_first));
  GroupExpression *grexp_b_first =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&operator_expression_b_first));

  // sets row counts for both tables in the child groups for A first
  optimizer_context.GetMemo()
        .GetGroupByID(grexp_a_first->GetChildGroupId(0))
        ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1)))->GetNumRows());
  optimizer_context.GetMemo()
        .GetGroupByID(grexp_a_first->GetChildGroupId(1))
        ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(2)))->GetNumRows());

  // sets row counts for both tables in the child groups for B first
  optimizer_context.GetMemo()
        .GetGroupByID(grexp_b_first->GetChildGroupId(0))
        ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(2)))->GetNumRows());
  optimizer_context.GetMemo()
        .GetGroupByID(grexp_b_first->GetChildGroupId(1))
        ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1)))->GetNumRows());

  auto cost_a_first = cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(),
      grexp_a_first);
  auto cost_b_first = cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(),
      grexp_b_first);

  // since B is much smaller, we expect that the cost of having it as the outer table is smaller
  EXPECT_LT(cost_b_first, cost_a_first);

  delete grexp_a_first;
  delete grexp_b_first;
  delete expr_b_1;
}

}  // namespace terrier::optimizer
