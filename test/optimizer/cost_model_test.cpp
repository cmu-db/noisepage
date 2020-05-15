#include <utility>
#include <random>

#include "gtest/gtest.h"
#include "optimizer/cost_model/cost_model.h"
#include "optimizer/optimizer_context.h"
#include "optimizer/physical_operators.h"
#include "optimizer/statistics/histogram.h"
#include "optimizer/statistics/top_k_elements.h"
#include "optimizer/optimizer_defs.h"
#include "execution/compiler/compiler.h"
#include "execution/compiler/expression_util.h"

#include "test_util/test_harness.h"

namespace terrier::optimizer {
class CostModelTests : public TerrierTest {
 protected:
  // A, B, C, D, E are tables with one column each.
  ColumnStats column_stats_obj_a_1_;
  ColumnStats column_stats_obj_b_1_;
  ColumnStats column_stats_obj_c_1_;
  ColumnStats column_stats_obj_d_1_;
  ColumnStats column_stats_obj_e_1_;
  TableStats table_stats_obj_a_;
  TableStats table_stats_obj_b_;
  TableStats table_stats_obj_c_;
  TableStats table_stats_obj_d_;
  TableStats table_stats_obj_e_;
  StatsStorage stats_storage_;
  CostModel cost_model_;

  static constexpr size_t HIST_BINS = 100;
  static constexpr size_t NUM_ROWS_A = 100'000;
  static constexpr size_t NUM_ROWS_B = 5;
  static constexpr size_t NUM_ROWS_C = 200;
  static constexpr size_t NUM_ROWS_D = 200;
  static constexpr size_t NUM_ROWS_E = 200'000;

  /** Boilerplate code to create ColumnStats from a histogram and TopK object.
   *
   * Assumes frac_null = 0.0 and cardinality is the same as number of rows.
   *
   * @param hist Histogram of values
   * @param top_k Top k values (also stores their estimated counts)
   * @param count Total count of elements for this dataset
   * @return new ColumnStats object for this dataset
   */
  static ColumnStats CreateColumnStats(catalog::db_oid_t database_id, catalog::table_oid_t table_id,
      catalog::col_oid_t col_id, Histogram<int> hist, TopKElements<int> top_k, size_t count) {

    std::vector<double> hist_bounds = hist.Uniform();
    auto most_common_keys = top_k.GetSortedTopKeys();
    std::vector<double> most_common_vals = {};
    std::vector<double> most_common_freqs = {};
    for (auto key : most_common_keys) {
      most_common_vals.push_back(static_cast<double>(key));
      most_common_freqs.push_back(static_cast<double>(top_k.EstimateItemCount(key)));
    }

    /* Args to ColumnStats constructor
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
    return ColumnStats(database_id, table_id, col_id, count, count, 0.0, std::move(most_common_vals),
                                        std::move(most_common_freqs), std::move(hist_bounds), true);
  }

  void SetUp() override {
    // generate 100k random values and generate histogram and most common val/freq stats for them.
    const int k = 10;
    const int count_min_sketch_width = 100;

    // generate uniform distribution
    Histogram<int> uniform_hist{HIST_BINS};
    TopKElements<int> uniform_top_k(k, count_min_sketch_width);
    std::default_random_engine uniform_generator;
    std::uniform_int_distribution<int> uniform_distribution(1, 1000);
    for (unsigned int i = 0; i < NUM_ROWS_A; i++) {
      int number = static_cast<int>(uniform_distribution(uniform_generator));
      uniform_hist.Increment(number);
      uniform_top_k.Increment(number, 1);
    }
    // generate gaussian (normal) distribution
    Histogram<int> normal_hist{HIST_BINS};
    TopKElements<int> normal_top_k(k, count_min_sketch_width);
    std::random_device rd;
    std::mt19937 non_uniform_generator(rd());
    std::normal_distribution<> norm_distribution(0, 10);
    for (unsigned int i = 0; i < NUM_ROWS_A; i++) {
      int number = static_cast<int>(norm_distribution(non_uniform_generator));
      normal_hist.Increment(number);
      normal_top_k.Increment(number, 1);
    }

    column_stats_obj_a_1_ = CreateColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(1),
        uniform_hist, uniform_top_k, NUM_ROWS_A);
    column_stats_obj_b_1_ = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(2), catalog::col_oid_t(1),
                                      NUM_ROWS_B, NUM_ROWS_B, 0.0, {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
    column_stats_obj_c_1_ = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(3), catalog::col_oid_t(1),
                                        NUM_ROWS_C, NUM_ROWS_C, 0.0, {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
    column_stats_obj_d_1_ = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(4), catalog::col_oid_t(1),
                                        NUM_ROWS_D, NUM_ROWS_D, 0.0, {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
    column_stats_obj_e_1_ = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(5), catalog::col_oid_t(1),
                                        NUM_ROWS_E, NUM_ROWS_E, 0.0, {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
    table_stats_obj_a_ = TableStats(
        catalog::db_oid_t(1), catalog::table_oid_t(1), NUM_ROWS_A, true,
        {column_stats_obj_a_1_});
    table_stats_obj_b_ = TableStats(
        catalog::db_oid_t(1), catalog::table_oid_t(2), NUM_ROWS_B, true,
        {column_stats_obj_b_1_});
    table_stats_obj_c_ = TableStats(
        catalog::db_oid_t(1), catalog::table_oid_t(3), NUM_ROWS_C, true,
        {column_stats_obj_c_1_});
    table_stats_obj_d_ = TableStats(
        catalog::db_oid_t(1), catalog::table_oid_t(4), NUM_ROWS_D, true,
        {column_stats_obj_d_1_});
    table_stats_obj_e_ = TableStats(
        catalog::db_oid_t(1), catalog::table_oid_t(5), NUM_ROWS_E, true,
        {column_stats_obj_e_1_});
    stats_storage_ = StatsStorage();
    stats_storage_.InsertTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1), std::move(table_stats_obj_a_));
    stats_storage_.InsertTableStats(catalog::db_oid_t(1), catalog::table_oid_t(2), std::move(table_stats_obj_b_));
    stats_storage_.InsertTableStats(catalog::db_oid_t(1), catalog::table_oid_t(3), std::move(table_stats_obj_c_));
    stats_storage_.InsertTableStats(catalog::db_oid_t(1), catalog::table_oid_t(4), std::move(table_stats_obj_d_));
    stats_storage_.InsertTableStats(catalog::db_oid_t(1), catalog::table_oid_t(5), std::move(table_stats_obj_e_));
    cost_model_ = CostModel();
    cost_model_.SetStatsStorage(&stats_storage_);
  }
};

// NOLINTNEXTLINE
TEST_F(CostModelTests, InnerNLJoinOrderTest) {
  OptimizerContext optimizer_context =
    OptimizerContext(common::ManagedPointer<AbstractCostModel>(&cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
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
  grexp_a_first->SetGroupID(group_id_t(0));
  GroupExpression *grexp_b_first =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&operator_expression_b_first));
  grexp_b_first->SetGroupID(group_id_t(0));

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
}

TEST_F(CostModelTests, HashVsNLJoinTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  Operator inner_nl_join = NLJoin::Make(PhysicalJoinType::INNER, std::vector<AnnotatedExpression>());
  Operator inner_hash_join = InnerHashJoin::Make(std::vector<AnnotatedExpression>(),
      std::vector<common::ManagedPointer<parser::AbstractExpression>>(),
      std::vector<common::ManagedPointer<parser::AbstractExpression>>());
  // create scan operators for each table
  Operator seq_scan_a = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), catalog::table_oid_t(1),
                                      std::vector<AnnotatedExpression>(), "table", false);
  Operator seq_scan_b = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), catalog::table_oid_t(2),
                                      std::vector<AnnotatedExpression>(), "table", false);
  std::vector<std::unique_ptr<OperatorNode>> children_nl_join = {};
  children_nl_join.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan_a, {})));
  children_nl_join.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan_b, {})));

  std::vector<std::unique_ptr<OperatorNode>> children_hash_join = {};
  children_hash_join.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan_b, {})));
  children_hash_join.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan_a, {})));

  // create operators/group expressions for NL join and hash join
  OperatorNode op_expr_nl_join = OperatorNode(inner_nl_join, std::move(children_nl_join));
  OperatorNode op_expr_hash_join = OperatorNode(inner_hash_join, std::move(children_hash_join));
  GroupExpression *grexp_nl_join =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&op_expr_nl_join));
  grexp_nl_join->SetGroupID(group_id_t(0));
  GroupExpression *grexp_hash_join =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&op_expr_hash_join));
  grexp_hash_join->SetGroupID(group_id_t(0));

  // sets row counts for both tables in the child groups for NL join
  optimizer_context.GetMemo()
      .GetGroupByID(grexp_nl_join->GetChildGroupId(0))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1)))->GetNumRows());
  optimizer_context.GetMemo()
      .GetGroupByID(grexp_nl_join->GetChildGroupId(1))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(2)))->GetNumRows());

  // sets row counts for both tables in the child groups for hash join
  optimizer_context.GetMemo()
      .GetGroupByID(grexp_hash_join->GetChildGroupId(0))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1)))->GetNumRows());
  optimizer_context.GetMemo()
      .GetGroupByID(grexp_hash_join->GetChildGroupId(1))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(2)))->GetNumRows());

  auto cost_nl_join = cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(),
                                                grexp_nl_join);
  auto cost_hash_join = cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(),
                                                grexp_hash_join);

  EXPECT_LT(cost_hash_join, cost_nl_join);

  delete grexp_nl_join;
  delete grexp_hash_join;
}

TEST_F(CostModelTests, HashVsNlJoinWithPredicateTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  //
  execution::compiler::ExpressionMaker expr_maker;
  // Get Table columns
  auto col1 = expr_maker.CVE(catalog::col_oid_t(1), type::TypeId::INTEGER);
  // Make predicate: x < 1 || x >= 3
  auto param2 = expr_maker.PVE(type::TypeId::INTEGER, 1);
  auto param3 = expr_maker.PVE(type::TypeId::INTEGER, 3);
  auto comp1 = expr_maker.ComparisonLt(col1, param2);
  auto comp2 = expr_maker.ComparisonGe(col1, param3);
  auto predicate = expr_maker.ConjunctionOr(comp1, comp2);
  AnnotatedExpression ann_predicate = AnnotatedExpression(predicate, std::unordered_set<std::string>());
  // Create join operators with predicate
  Operator inner_nl_join = NLJoin::Make(PhysicalJoinType::INNER, std::vector<AnnotatedExpression>{ann_predicate});
  Operator inner_hash_join = InnerHashJoin::Make(std::vector<AnnotatedExpression>{ann_predicate},
                                                 std::vector<common::ManagedPointer<parser::AbstractExpression>>(),
                                                 std::vector<common::ManagedPointer<parser::AbstractExpression>>());
  // child operators: one scans the first table, while the other scans the 2nd table. The join will operate on these two
  // tables.
  Operator seq_scan_a = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), catalog::table_oid_t(1),
                                      std::vector<AnnotatedExpression>(), "table", false);
  Operator seq_scan_b = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), catalog::table_oid_t(2),
                                      std::vector<AnnotatedExpression>(), "table", false);
  std::vector<std::unique_ptr<OperatorNode>> children_nl_join = {};
  children_nl_join.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan_a, {})));
  children_nl_join.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan_b, {})));

  std::vector<std::unique_ptr<OperatorNode>> children_hash_join = {};
  children_hash_join.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan_b, {})));
  children_hash_join.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan_a, {})));

  OperatorNode op_expr_nl_join = OperatorNode(inner_nl_join, std::move(children_nl_join));
  OperatorNode op_expr_hash_join = OperatorNode(inner_hash_join, std::move(children_hash_join));
  GroupExpression *grexp_nl_join =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&op_expr_nl_join));
  grexp_nl_join->SetGroupID(group_id_t(0));
  GroupExpression *grexp_hash_join =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&op_expr_hash_join));
  grexp_hash_join->SetGroupID(group_id_t(0));

  // sets row counts for both tables in the child groups for A first
  optimizer_context.GetMemo()
      .GetGroupByID(grexp_nl_join->GetChildGroupId(0))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1)))->GetNumRows());
  optimizer_context.GetMemo()
      .GetGroupByID(grexp_nl_join->GetChildGroupId(1))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(2)))->GetNumRows());

  // sets row counts for both tables in the child groups for B first
  optimizer_context.GetMemo()
      .GetGroupByID(grexp_hash_join->GetChildGroupId(0))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1)))->GetNumRows());
  optimizer_context.GetMemo()
      .GetGroupByID(grexp_hash_join->GetChildGroupId(1))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(2)))->GetNumRows());

  auto cost_nl_join = cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(),
                                                grexp_nl_join);
  auto cost_hash_join = cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(),
                                                  grexp_hash_join);

  EXPECT_LT(cost_hash_join, cost_nl_join);

  delete grexp_nl_join;
  delete grexp_hash_join;

}

TEST_F(CostModelTests, HashVsNlJoinWithPredicateSmallTablesTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  //
  execution::compiler::ExpressionMaker expr_maker;
  // Get Table columns
  auto col1 = expr_maker.CVE(catalog::col_oid_t(1), type::TypeId::INTEGER);
  // Make predicate: x < 2
  auto param1 = expr_maker.PVE(type::TypeId::INTEGER, 2);
  auto predicate = expr_maker.ComparisonLt(col1, param1);
  AnnotatedExpression ann_predicate = AnnotatedExpression(predicate, std::unordered_set<std::string>());
  // Create join operators with predicate
  Operator inner_nl_join = NLJoin::Make(PhysicalJoinType::INNER, std::vector<AnnotatedExpression>{ann_predicate});
  Operator inner_hash_join = InnerHashJoin::Make(std::vector<AnnotatedExpression>{ann_predicate},
                                                 std::vector<common::ManagedPointer<parser::AbstractExpression>>(),
                                                 std::vector<common::ManagedPointer<parser::AbstractExpression>>());

  // child operators for scanning C, D (200 rows each) - should use NL join
  Operator seq_scan_c = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), catalog::table_oid_t(3),
                                      std::vector<AnnotatedExpression>(), "table", false);
  Operator seq_scan_d = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), catalog::table_oid_t(4),
                                      std::vector<AnnotatedExpression>(), "table", false);

  std::vector<std::unique_ptr<OperatorNode>> children_nl_join_small = {};
  children_nl_join_small.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan_c, {})));
  children_nl_join_small.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan_d, {})));
  std::vector<std::unique_ptr<OperatorNode>> children_hash_join_small = {};
  children_hash_join_small.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan_c, {})));
  children_hash_join_small.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan_d, {})));

  OperatorNode op_expr_nl_join_small = OperatorNode(inner_nl_join, std::move(children_nl_join_small));
  OperatorNode op_expr_hash_join_small = OperatorNode(inner_hash_join, std::move(children_hash_join_small));
  GroupExpression *grexp_nl_join_small =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&op_expr_nl_join_small));
  grexp_nl_join_small->SetGroupID(group_id_t(0));
  GroupExpression *grexp_hash_join_small =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&op_expr_hash_join_small));
  grexp_hash_join_small->SetGroupID(group_id_t(0));

  // sets row counts for both tables in the child groups for NL join small tables C, D
  optimizer_context.GetMemo()
      .GetGroupByID(grexp_nl_join_small->GetChildGroupId(0))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(3)))->GetNumRows());
  optimizer_context.GetMemo()
      .GetGroupByID(grexp_nl_join_small->GetChildGroupId(1))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(4)))->GetNumRows());

  // sets row counts for both tables in the child groups for hash join small tables C, D
  optimizer_context.GetMemo()
      .GetGroupByID(grexp_hash_join_small->GetChildGroupId(0))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(3)))->GetNumRows());
  optimizer_context.GetMemo()
      .GetGroupByID(grexp_hash_join_small->GetChildGroupId(1))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(4)))->GetNumRows());

  auto cost_nl_join_small = cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(),
                                                grexp_nl_join_small);
  auto cost_hash_join_small = cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(),
                                                  grexp_hash_join_small);

  EXPECT_LT(cost_nl_join_small, cost_hash_join_small);

  delete grexp_nl_join_small;
  delete grexp_hash_join_small;
}

TEST_F(CostModelTests, HashVsNlJoinWithPredicateLargeTablesTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  //
  execution::compiler::ExpressionMaker expr_maker;
  // Get Table columns
  auto col1 = expr_maker.CVE(catalog::col_oid_t(1), type::TypeId::INTEGER);
  // Make predicate: x < 2
  auto param1 = expr_maker.PVE(type::TypeId::INTEGER, 2);
  auto predicate = expr_maker.ComparisonLt(col1, param1);
  AnnotatedExpression ann_predicate = AnnotatedExpression(predicate, std::unordered_set<std::string>());
  // Create join operators with predicate
  Operator inner_nl_join = NLJoin::Make(PhysicalJoinType::INNER, std::vector<AnnotatedExpression>{ann_predicate});
  Operator inner_hash_join = InnerHashJoin::Make(std::vector<AnnotatedExpression>{ann_predicate},
                                                 std::vector<common::ManagedPointer<parser::AbstractExpression>>(),
                                                 std::vector<common::ManagedPointer<parser::AbstractExpression>>());

  // child operators for scanning A, E (100k, 200k rows respectively) - should use hash join
  Operator seq_scan_c = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), catalog::table_oid_t(3),
                                      std::vector<AnnotatedExpression>(), "table", false);
  Operator seq_scan_d = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), catalog::table_oid_t(4),
                                      std::vector<AnnotatedExpression>(), "table", false);

  std::vector<std::unique_ptr<OperatorNode>> children_nl_join_large = {};
  children_nl_join_large.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan_c, {})));
  children_nl_join_large.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan_d, {})));
  std::vector<std::unique_ptr<OperatorNode>> children_hash_join_large = {};
  children_hash_join_large.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan_c, {})));
  children_hash_join_large.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan_d, {})));

  OperatorNode op_expr_nl_join_large = OperatorNode(inner_nl_join, std::move(children_nl_join_large));
  OperatorNode op_expr_hash_join_large = OperatorNode(inner_hash_join, std::move(children_hash_join_large));
  GroupExpression *grexp_nl_join_large =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&op_expr_nl_join_large));
  grexp_nl_join_large->SetGroupID(group_id_t(0));
  GroupExpression *grexp_hash_join_large =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&op_expr_hash_join_large));
  grexp_hash_join_large->SetGroupID(group_id_t(0));

  // sets row counts for both tables in the child groups for NL join large tables A, E
  optimizer_context.GetMemo()
      .GetGroupByID(grexp_nl_join_large->GetChildGroupId(0))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1)))->GetNumRows());
  optimizer_context.GetMemo()
      .GetGroupByID(grexp_nl_join_large->GetChildGroupId(1))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(5)))->GetNumRows());

  // sets row counts for both tables in the child groups for hash join large tables A, E
  optimizer_context.GetMemo()
      .GetGroupByID(grexp_hash_join_large->GetChildGroupId(0))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1)))->GetNumRows());
  optimizer_context.GetMemo()
      .GetGroupByID(grexp_hash_join_large->GetChildGroupId(1))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(5)))->GetNumRows());

  auto cost_nl_join_large = cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(),
                                                      grexp_nl_join_large);
  auto cost_hash_join_large = cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(),
                                                        grexp_hash_join_large);

  EXPECT_LT(cost_hash_join_large, cost_nl_join_large);

  delete grexp_nl_join_large;
  delete grexp_hash_join_large;
}

}  // namespace terrier::optimizer
