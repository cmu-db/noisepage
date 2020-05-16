#include "optimizer/cost_model/cost_model.h"
#include <utility>
#include "execution/compiler/expression_util.h"
#include "gtest/gtest.h"
#include "optimizer/optimizer_context.h"
#include "optimizer/optimizer_defs.h"
#include "optimizer/physical_operators.h"
#include "optimizer/statistics/histogram.h"
#include "optimizer/statistics/top_k_elements.h"

#include "test_util/test_harness.h"

namespace terrier::optimizer {
class CostModelTests : public TerrierTest {
 protected:
  static constexpr size_t NUM_ROWS_A = 100'000;
  static constexpr size_t NUM_ROWS_B = 5;
  static constexpr size_t NUM_ROWS_C = 5;
  static constexpr size_t NUM_ROWS_D = 5;
  static constexpr size_t NUM_ROWS_E = 100'000;

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
  void SetUp() override {
    /////// COLUMNS //////
    // table 1 column stats
    column_stats_obj_a_1_ = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(1),
                                        NUM_ROWS_A, NUM_ROWS_A / 2.0, 0.2, {1, 2, 3}, {5, 5, 5}, {1.0, 5.0}, true);

    // table 2 column stats
    column_stats_obj_b_1_ = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(2), catalog::col_oid_t(1),
                                        NUM_ROWS_B, NUM_ROWS_B, 0.0, {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);

    // table 3 column stats
    column_stats_obj_c_1_ = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(3), catalog::col_oid_t(1),
                                        NUM_ROWS_C, NUM_ROWS_C, 0.0, {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);

    // table 4 column stats
    column_stats_obj_d_1_ = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(4), catalog::col_oid_t(1),
                                        NUM_ROWS_D, NUM_ROWS_D, 0.0, {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);

    // table 5 column stats
    column_stats_obj_e_1_ = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(5), catalog::col_oid_t(1),
                                        NUM_ROWS_E, NUM_ROWS_E / 2.0, 0.0, {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);

    ////// TABLES //////
    \
    // table 1
    table_stats_obj_a_ =
        TableStats(catalog::db_oid_t(1), catalog::table_oid_t(1), NUM_ROWS_A, true, {column_stats_obj_a_1_});
    // table 2
    table_stats_obj_b_ =
        TableStats(catalog::db_oid_t(1), catalog::table_oid_t(2), NUM_ROWS_B, true, {column_stats_obj_b_1_});
    // table 3
    table_stats_obj_c_ =
        TableStats(catalog::db_oid_t(1), catalog::table_oid_t(3), NUM_ROWS_C, true, {column_stats_obj_c_1_});
    // table 4
    table_stats_obj_d_ =
        TableStats(catalog::db_oid_t(1), catalog::table_oid_t(4), NUM_ROWS_D, true, {column_stats_obj_d_1_});
    // table 5
    table_stats_obj_e_ =
        TableStats(catalog::db_oid_t(1), catalog::table_oid_t(5), NUM_ROWS_E, true, {column_stats_obj_e_1_});

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
TEST_F(CostModelTests, InnerNLJoinCorrectnessTest) {
  OptimizerContext context_ = OptimizerContext(common::ManagedPointer<AbstractCostModel>(&cost_model_));
  context_.SetStatsStorage(&stats_storage_);
  // create child gexprs
  auto seq_scan_1 = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), catalog::table_oid_t(1),
                                  std::vector<AnnotatedExpression>(), "table", false);
  auto seq_scan_2 = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), catalog::table_oid_t(2),
                                  std::vector<AnnotatedExpression>(), "table", false);

  std::vector<std::unique_ptr<OperatorNode>> children_larger_outer = {};
  children_larger_outer.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan_1, {})));
  children_larger_outer.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan_2, {})));

  Operator inner_nl_join_a_first = InnerNLJoin::Make(std::vector<AnnotatedExpression>());
  OperatorNode operator_expression_a_first = OperatorNode(inner_nl_join_a_first, std::move(children_larger_outer));
  auto gexpr_inner_nl_join =
      context_.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&operator_expression_a_first));
  context_.GetMemo().InsertExpression(gexpr_inner_nl_join, false);
  auto left_group = context_.GetMemo().GetGroupByID(group_id_t(0));
  auto right_group = context_.GetMemo().GetGroupByID(group_id_t(1));
  auto curr_group = context_.GetMemo().GetGroupByID(group_id_t(2));
  left_group->SetNumRows(table_stats_obj_a_.GetNumRows());
  right_group->SetNumRows(table_stats_obj_b_.GetNumRows());
  curr_group->SetNumRows(1000);
  auto left_gexpr = left_group->GetPhysicalExpressions()[0];
  auto right_gexpr = right_group->GetPhysicalExpressions()[0];
  auto prop_set = new PropertySet();
  left_group->SetExpressionCost(left_gexpr, cost_model_.CalculateCost(nullptr, &context_.GetMemo(), left_gexpr),
                                prop_set);
  right_group->SetExpressionCost(right_gexpr, cost_model_.CalculateCost(nullptr, &context_.GetMemo(), right_gexpr),
                                 prop_set);
  auto cost_larger_outer = cost_model_.CalculateCost(nullptr, &context_.GetMemo(), gexpr_inner_nl_join);

  std::vector<std::unique_ptr<OperatorNode>> children_smaller_outer = {};
  children_smaller_outer.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan_2, {})));
  children_smaller_outer.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan_1, {})));

  Operator inner_nl_join_a_second = InnerNLJoin::Make(std::vector<AnnotatedExpression>());
  OperatorNode operator_expression_a_second = OperatorNode(inner_nl_join_a_second, std::move(children_smaller_outer));
  auto gexpr_inner_nl_join_2 =
      context_.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&operator_expression_a_second));
  context_.GetMemo().InsertExpression(gexpr_inner_nl_join_2, false);
  auto curr_group_2 = context_.GetMemo().GetGroupByID(group_id_t(3));
  curr_group_2->SetNumRows(1000);
  auto cost_smaller_outer = cost_model_.CalculateCost(nullptr, &context_.GetMemo(), gexpr_inner_nl_join_2);

  EXPECT_EQ(cost_smaller_outer, cost_larger_outer);
  delete prop_set;
}

TEST_F(CostModelTests, HashJoinCorrectnessTest) {
  OptimizerContext context_ = OptimizerContext(common::ManagedPointer<AbstractCostModel>(&cost_model_));
  context_.SetStatsStorage(&stats_storage_);
  // create child gexprs
  auto seq_scan_1 = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), catalog::table_oid_t(1),
                                  std::vector<AnnotatedExpression>(), "table", false);
  auto seq_scan_2 = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), catalog::table_oid_t(2),
                                  std::vector<AnnotatedExpression>(), "table", false);

  execution::compiler::ExpressionMaker expr_maker;
  // Get Table columns
  auto col1 = expr_maker.MakeManaged(std::make_unique<parser::ColumnValueExpression>(
      catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(1)));
  auto col2 = expr_maker.MakeManaged(std::make_unique<parser::ColumnValueExpression>(
      catalog::db_oid_t(1), catalog::table_oid_t(2), catalog::col_oid_t(1)));

  // Make predicate: x.col1 == y.col1
  auto predicate = expr_maker.ComparisonEq(col1, col2);
  AnnotatedExpression ann_predicate = AnnotatedExpression(predicate, std::unordered_set<std::string>());

  std::vector<std::unique_ptr<OperatorNode>> children_larger_outer = {};
  children_larger_outer.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan_1, {})));
  children_larger_outer.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan_2, {})));

  // make first inner hash join

  Operator inner_hash_join_a_first =
      InnerHashJoin::Make(std::vector<AnnotatedExpression>{ann_predicate},
                          std::vector<common::ManagedPointer<parser::AbstractExpression>>{
                              common::ManagedPointer<parser::AbstractExpression>(col1.Get())},
                          std::vector<common::ManagedPointer<parser::AbstractExpression>>{
                              common::ManagedPointer<parser::AbstractExpression>(col2.Get())});
  OperatorNode operator_expression_a_first = OperatorNode(inner_hash_join_a_first, std::move(children_larger_outer));
  auto gexpr_inner_hash_join =
      context_.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&operator_expression_a_first));

  context_.GetMemo().InsertExpression(gexpr_inner_hash_join, false);
  auto left_group = context_.GetMemo().GetGroupByID(group_id_t(0));
  auto right_group = context_.GetMemo().GetGroupByID(group_id_t(1));
  auto curr_group = context_.GetMemo().GetGroupByID(group_id_t(2));
  left_group->SetNumRows(table_stats_obj_a_.GetNumRows());
  right_group->SetNumRows(table_stats_obj_b_.GetNumRows());
  curr_group->SetNumRows(1000);

  auto left_gexpr = left_group->GetPhysicalExpressions()[0];
  auto right_gexpr = right_group->GetPhysicalExpressions()[0];
  auto prop_set = new PropertySet();

  left_group->SetExpressionCost(left_gexpr, cost_model_.CalculateCost(nullptr, &context_.GetMemo(), left_gexpr),
                                prop_set);
  right_group->SetExpressionCost(right_gexpr, cost_model_.CalculateCost(nullptr, &context_.GetMemo(), right_gexpr),
                                 prop_set);
  auto hash_join_1_cost = cost_model_.CalculateCost(nullptr, &context_.GetMemo(), gexpr_inner_hash_join);

  EXPECT_EQ(hash_join_1_cost, 1002020);

  delete prop_set;
}

}  // namespace terrier::optimizer
