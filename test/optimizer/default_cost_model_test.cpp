#include <utility>

#include "gtest/gtest.h"
#include "optimizer/cost_model/default_cost_model.h"
#include "optimizer/optimizer_context.h"
#include "optimizer/physical_operators.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression/operator_expression.h"
#include "type/transient_value.h"
#include "type/transient_value_factory.h"

#include "test_util/test_harness.h"

namespace terrier::optimizer {
class DefaultCostModelTests : public TerrierTest {
 protected:
  ColumnStats column_stats_obj_1_;
  ColumnStats column_stats_obj_2_;
  ColumnStats column_stats_obj_3_;
  ColumnStats column_stats_obj_4_;
  ColumnStats column_stats_obj_5_;
  TableStats table_stats_obj_1_;
  ColumnStats column_stats_obj_6_;
  ColumnStats column_stats_obj_7_;
  ColumnStats column_stats_obj_8_;
  ColumnStats column_stats_obj_9_;
  ColumnStats column_stats_obj_10_;
  TableStats table_stats_obj_2_;
  StatsStorage stats_storage_;
  DefaultCostModel default_cost_model_;

  void SetUp() override {
    column_stats_obj_1_ = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(1), 5, 4, 0.2,
                                      {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
    column_stats_obj_2_ = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(2), 5, 4, 0.2,
                                      {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
    column_stats_obj_3_ = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(3), 5, 4, 0.2,
                                      {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
    column_stats_obj_4_ = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(4), 5, 4, 0.2,
                                      {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
    column_stats_obj_5_ = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(5), 5, 4, 0.2,
                                      {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
    table_stats_obj_1_ = TableStats(
        catalog::db_oid_t(1), catalog::table_oid_t(1), 5, true,
        {column_stats_obj_1_, column_stats_obj_2_, column_stats_obj_3_, column_stats_obj_4_, column_stats_obj_5_});
    column_stats_obj_6_ = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(2), catalog::col_oid_t(1), 10, 4, 0.2,
                                      {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
    column_stats_obj_7_ = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(2), catalog::col_oid_t(2), 10, 4, 0.2,
                                      {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
    column_stats_obj_8_ = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(2), catalog::col_oid_t(3), 10, 4, 0.2,
                                      {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
    column_stats_obj_9_ = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(2), catalog::col_oid_t(4), 10, 4, 0.2,
                                      {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
    column_stats_obj_10_ = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(2), catalog::col_oid_t(5), 10, 4, 0.2,
                                       {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
    table_stats_obj_2_ = TableStats(
        catalog::db_oid_t(1), catalog::table_oid_t(2), 10, true,
        {column_stats_obj_6_, column_stats_obj_7_, column_stats_obj_8_, column_stats_obj_9_, column_stats_obj_10_});
    stats_storage_ = StatsStorage();
    stats_storage_.InsertTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1), std::move(table_stats_obj_1_));
    stats_storage_.InsertTableStats(catalog::db_oid_t(1), catalog::table_oid_t(2), std::move(table_stats_obj_2_));
    default_cost_model_ = DefaultCostModel();
    default_cost_model_.SetStatsStorage(&stats_storage_);
  };
};

// NOLINTNEXTLINE
TEST_F(DefaultCostModelTests, SeqScanTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&default_cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  Operator seq_scan = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(1),
                                    std::vector<AnnotatedExpression>(), "table", false);
  OperatorExpression operator_expression = OperatorExpression(seq_scan, {});
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorExpression>(&operator_expression));
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  EXPECT_EQ(cost, 0.05);
}

// NOLINTNEXTLINE
TEST_F(DefaultCostModelTests, IndexScanTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&default_cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  Operator index_scan = IndexScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(1),
                                        catalog::index_oid_t(1), std::vector<AnnotatedExpression>(), true,
                                        planner::IndexScanType::AscendingClosed, {});
  OperatorExpression operator_expression = OperatorExpression(index_scan, {});
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorExpression>(&operator_expression));
  grexp->SetGroupID(group_id_t(0));
  optimizer_context.GetMemo().InsertExpression(grexp, true);
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  EXPECT_EQ(cost, std::log2(5) * 0.005 + optimizer_context.GetMemo().GetGroupByID(group_id_t(0))->GetNumRows() * 0.01);
}

// NOLINTNEXTLINE
TEST_F(DefaultCostModelTests, QueryDerivedScanTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&default_cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  auto alias_to_expr_map = std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>>();
  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TransientValueFactory::GetTinyInt(1));
  auto expr1 = common::ManagedPointer(expr_b_1);
  alias_to_expr_map["constant expr"] = expr1;
  Operator query_derived_scan = QueryDerivedScan::Make("table", std::move(alias_to_expr_map));
  OperatorExpression operator_expression = OperatorExpression(query_derived_scan, {});
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorExpression>(&operator_expression));
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  EXPECT_EQ(cost, 0.f);
}

// NOLINTNEXTLINE
TEST_F(DefaultCostModelTests, OrderByTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&default_cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  Operator order_by = OrderBy::Make();
  Operator seq_scan = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(1),
                                    std::vector<AnnotatedExpression>(), "table", false);
  std::vector<std::unique_ptr<OperatorExpression>> children = {};
  children.push_back(std::make_unique<OperatorExpression>(OperatorExpression(seq_scan, {})));
  OperatorExpression operator_expression = OperatorExpression(order_by, std::move(children));
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorExpression>(&operator_expression));
  optimizer_context.GetMemo()
      .GetGroupByID(grexp->GetChildGroupId(0))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1)))->GetNumRows());
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  EXPECT_EQ(cost, 5 * std::log2(5) * 0.01);
}

// NOLINTNEXTLINE
TEST_F(DefaultCostModelTests, LimitTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&default_cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  Operator limit = Limit::Make(0, 3, {}, {});
  Operator seq_scan = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(1),
                                    std::vector<AnnotatedExpression>(), "table", false);
  std::vector<std::unique_ptr<OperatorExpression>> children = {};
  children.push_back(std::make_unique<OperatorExpression>(OperatorExpression(seq_scan, {})));
  OperatorExpression operator_expression = OperatorExpression(limit, std::move(children));
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorExpression>(&operator_expression));
  optimizer_context.GetMemo()
      .GetGroupByID(grexp->GetChildGroupId(0))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1)))->GetNumRows());
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  EXPECT_EQ(cost, std::min(5, 3) * 0.01);
}

// NOLINTNEXTLINE
TEST_F(DefaultCostModelTests, InnerNLJoinTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&default_cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto annotated_expr_0 =
      AnnotatedExpression(common::ManagedPointer<parser::AbstractExpression>(), std::unordered_set<std::string>());
  Operator inner_nl_join = InnerNLJoin::Make(std::vector<AnnotatedExpression>(), {x_1}, {x_1});
  Operator seq_scan_1 = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), catalog::table_oid_t(1),
                                      std::vector<AnnotatedExpression>(), "table", false);
  Operator seq_scan_2 = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), catalog::table_oid_t(2),
                                      std::vector<AnnotatedExpression>(), "table", false);
  std::vector<std::unique_ptr<OperatorExpression>> children = {};
  children.push_back(std::make_unique<OperatorExpression>(OperatorExpression(seq_scan_1, {})));
  children.push_back(std::make_unique<OperatorExpression>(OperatorExpression(seq_scan_2, {})));
  OperatorExpression operator_expression = OperatorExpression(inner_nl_join, std::move(children));
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorExpression>(&operator_expression));
  optimizer_context.GetMemo()
      .GetGroupByID(grexp->GetChildGroupId(0))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1)))->GetNumRows());
  optimizer_context.GetMemo()
      .GetGroupByID(grexp->GetChildGroupId(1))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(2)))->GetNumRows());
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  EXPECT_EQ(cost, 0.5);
}

// NOLINTNEXTLINE
TEST_F(DefaultCostModelTests, LeftNLJoinTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&default_cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  Operator left_nl_join = LeftNLJoin::Make(x_1);
  OperatorExpression operator_expression = OperatorExpression(left_nl_join, {});
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorExpression>(&operator_expression));
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  EXPECT_EQ(cost, 0.f);
}

// NOLINTNEXTLINE
TEST_F(DefaultCostModelTests, RightNLJoinTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&default_cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  Operator right_nl_join = RightNLJoin::Make(x_1);
  OperatorExpression operator_expression = OperatorExpression(right_nl_join, {});
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorExpression>(&operator_expression));
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  EXPECT_EQ(cost, 0.f);
}

// NOLINTNEXTLINE
TEST_F(DefaultCostModelTests, OuterNLJoinTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&default_cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  Operator outer_nl_join = OuterNLJoin::Make(x_1);
  OperatorExpression operator_expression = OperatorExpression(outer_nl_join, {});
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorExpression>(&operator_expression));
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  EXPECT_EQ(cost, 0.f);
}

// NOLINTNEXTLINE
TEST_F(DefaultCostModelTests, LeftHashJoinTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&default_cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  Operator left_hash_join = LeftHashJoin::Make(x_1);
  OperatorExpression operator_expression = OperatorExpression(left_hash_join, {});
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorExpression>(&operator_expression));
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  EXPECT_EQ(cost, 0.f);
}

// NOLINTNEXTLINE
TEST_F(DefaultCostModelTests, RightHashJoinTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&default_cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  Operator right_hash_join = RightHashJoin::Make(x_1);
  OperatorExpression operator_expression = OperatorExpression(right_hash_join, {});
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorExpression>(&operator_expression));
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  EXPECT_EQ(cost, 0.f);
}

// NOLINTNEXTLINE
TEST_F(DefaultCostModelTests, OuterHashJoinTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&default_cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  Operator outer_hash_join = OuterHashJoin::Make(x_1);
  OperatorExpression operator_expression = OperatorExpression(outer_hash_join, {});
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorExpression>(&operator_expression));
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  EXPECT_EQ(cost, 0.f);
}

// NOLINTNEXTLINE
TEST_F(DefaultCostModelTests, InsertTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&default_cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  catalog::col_oid_t columns[] = {catalog::col_oid_t(1), catalog::col_oid_t(2)};
  std::vector<catalog::index_oid_t> indexes = {catalog::index_oid_t(4), catalog::index_oid_t(5)};
  parser::AbstractExpression *raw_values[] = {
      new parser::ConstantValueExpression(type::TransientValueFactory::GetTinyInt(1)),
      new parser::ConstantValueExpression(type::TransientValueFactory::GetTinyInt(9))};
  std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>> values = {
      std::vector<common::ManagedPointer<parser::AbstractExpression>>(raw_values, std::end(raw_values))};
  Operator insert = Insert::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(1),
                                 std::vector<catalog::col_oid_t>(columns, std::end(columns)),
                                 std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>>(values),
                                 std::vector<catalog::index_oid_t>(indexes));
  OperatorExpression operator_expression = OperatorExpression(insert, {});
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorExpression>(&operator_expression));
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  EXPECT_EQ(cost, 0.f);
}

// NOLINTNEXTLINE
TEST_F(DefaultCostModelTests, InsertSelectTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&default_cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  std::vector<catalog::index_oid_t> index_oids{1};
  Operator insert_select = InsertSelect::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1),
                                              catalog::table_oid_t(1), std::vector<catalog::index_oid_t>(index_oids));
  OperatorExpression operator_expression = OperatorExpression(insert_select, {});
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorExpression>(&operator_expression));
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  EXPECT_EQ(cost, 0.f);
}

// NOLINTNEXTLINE
TEST_F(DefaultCostModelTests, DeleteTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&default_cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  Operator delete_op =
      Delete::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), "table", catalog::table_oid_t(1));
  OperatorExpression operator_expression = OperatorExpression(delete_op, {});
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorExpression>(&operator_expression));
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  EXPECT_EQ(cost, 0.f);
}

// NOLINTNEXTLINE
TEST_F(DefaultCostModelTests, UpdateTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&default_cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  Operator update =
      Update::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), "table", catalog::table_oid_t(1), {});
  OperatorExpression operator_expression = OperatorExpression(update, {});
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorExpression>(&operator_expression));
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  EXPECT_EQ(cost, 0.f);
}

// NOLINTNEXTLINE
TEST_F(DefaultCostModelTests, HashGroupByTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&default_cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto annotated_expr_0 =
      AnnotatedExpression(common::ManagedPointer<parser::AbstractExpression>(), std::unordered_set<std::string>());
  Operator hash_group_by = HashGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_1},
                                             std::vector<AnnotatedExpression>{annotated_expr_0});
  Operator seq_scan = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(1),
                                    std::vector<AnnotatedExpression>(), "table", false);
  std::vector<std::unique_ptr<OperatorExpression>> children = {};
  children.push_back(std::make_unique<OperatorExpression>(OperatorExpression(seq_scan, {})));
  OperatorExpression operator_expression = OperatorExpression(hash_group_by, std::move(children));
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorExpression>(&operator_expression));
  optimizer_context.GetMemo()
      .GetGroupByID(grexp->GetChildGroupId(0))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1)))->GetNumRows());
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  EXPECT_EQ(cost, 10 * 0.01);
}

// NOLINTNEXTLINE
TEST_F(DefaultCostModelTests, SortGroupByTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&default_cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto annotated_expr_0 =
      AnnotatedExpression(common::ManagedPointer<parser::AbstractExpression>(), std::unordered_set<std::string>());
  Operator sort_group_by = SortGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_1},
                                             std::vector<AnnotatedExpression>{annotated_expr_0});
  Operator seq_scan = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(1),
                                    std::vector<AnnotatedExpression>(), "table", false);
  std::vector<std::unique_ptr<OperatorExpression>> children = {};
  children.push_back(std::make_unique<OperatorExpression>(OperatorExpression(seq_scan, {})));
  OperatorExpression operator_expression = OperatorExpression(sort_group_by, std::move(children));
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorExpression>(&operator_expression));
  optimizer_context.GetMemo()
      .GetGroupByID(grexp->GetChildGroupId(0))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1)))->GetNumRows());
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  EXPECT_EQ(cost, 5 * 0.01);
}

// NOLINTNEXTLINE
TEST_F(DefaultCostModelTests, AggregateTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&default_cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  Operator aggregate = Aggregate::Make();
  Operator seq_scan = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(1),
                                    std::vector<AnnotatedExpression>(), "table", false);
  std::vector<std::unique_ptr<OperatorExpression>> children = {};
  children.push_back(std::make_unique<OperatorExpression>(OperatorExpression(seq_scan, {})));
  OperatorExpression operator_expression = OperatorExpression(aggregate, std::move(children));
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorExpression>(&operator_expression));
  optimizer_context.GetMemo()
      .GetGroupByID(grexp->GetChildGroupId(0))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1)))->GetNumRows());
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  EXPECT_EQ(cost, 10 * 0.01);
}

}  // namespace terrier::optimizer
