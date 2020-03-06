#include <utility>

#include "gtest/gtest.h"
#include "optimizer/cost_model/default_cost_model.h"
#include "optimizer/operator_node.h"
#include "optimizer/optimizer_context.h"
#include "optimizer/physical_operators.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
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
    // creates a stats storage object with 2 table stats objects each containing 5 column stats objects
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
  // creates an optimizer context and adds the stats storage object we are using to it
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&default_cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  // creates seq scan operator
  Operator seq_scan = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(1),
                                    std::vector<AnnotatedExpression>(), "table", false);
  // creates operator node with above operator
  OperatorNode operator_expression = OperatorNode(seq_scan, {});
  // adds a group expression to optimizer context obj using the operator node
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&operator_expression));
  // calculates cost
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  ASSERT_FLOAT_EQ(cost, 0.05);
  delete grexp;
}

// NOLINTNEXTLINE
TEST_F(DefaultCostModelTests, IndexScanTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&default_cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  // create index scan operator
  Operator index_scan = IndexScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(1),
                                        catalog::index_oid_t(1), std::vector<AnnotatedExpression>(), true,
                                        planner::IndexScanType::AscendingClosed, {});
  OperatorNode operator_expression = OperatorNode(index_scan, {});
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&operator_expression));
  // add a group ID to the grexp created so that it can be identified when performing operations on it
  grexp->SetGroupID(group_id_t(0));
  optimizer_context.GetMemo().InsertExpression(grexp, true);
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  ASSERT_FLOAT_EQ(cost,
                  std::log2(5) * 0.005 + optimizer_context.GetMemo().GetGroupByID(group_id_t(0))->GetNumRows() * 0.01);
  delete grexp;
}

// NOLINTNEXTLINE
TEST_F(DefaultCostModelTests, QueryDerivedScanTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&default_cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  // create abstract expr in order to create query derived scan operator below
  auto alias_to_expr_map = std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>>();
  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TransientValueFactory::GetTinyInt(1));
  auto expr1 = common::ManagedPointer(expr_b_1);
  alias_to_expr_map["constant expr"] = expr1;
  Operator query_derived_scan = QueryDerivedScan::Make("table", std::move(alias_to_expr_map));
  OperatorNode operator_expression = OperatorNode(query_derived_scan, {});
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&operator_expression));
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  ASSERT_FLOAT_EQ(cost, 0.f);
  delete grexp;
  delete expr_b_1;
}

// NOLINTNEXTLINE
TEST_F(DefaultCostModelTests, OrderByTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&default_cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  // create order by operator
  Operator order_by = OrderBy::Make();
  // create child seq scan operator
  Operator seq_scan = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(1),
                                    std::vector<AnnotatedExpression>(), "table", false);
  // populate children node list with child operator
  std::vector<std::unique_ptr<OperatorNode>> children = {};
  children.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan, {})));
  OperatorNode operator_expression = OperatorNode(order_by, std::move(children));
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&operator_expression));
  // set the num rows of the table we are operating on in the group
  optimizer_context.GetMemo()
      .GetGroupByID(grexp->GetChildGroupId(0))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1)))->GetNumRows());
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  ASSERT_FLOAT_EQ(cost, 5 * std::log2(5) * 0.01);
  delete grexp;
}

// NOLINTNEXTLINE
TEST_F(DefaultCostModelTests, LimitTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&default_cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  Operator limit = Limit::Make(0, 3, {}, {});
  // make new child operator and push to list of children
  Operator seq_scan = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(1),
                                    std::vector<AnnotatedExpression>(), "table", false);
  std::vector<std::unique_ptr<OperatorNode>> children = {};
  children.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan, {})));
  OperatorNode operator_expression = OperatorNode(limit, std::move(children));
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&operator_expression));
  // set the num rows of the table we are operating on in the group
  optimizer_context.GetMemo()
      .GetGroupByID(grexp->GetChildGroupId(0))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1)))->GetNumRows());
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  ASSERT_FLOAT_EQ(cost, std::min(5, 3) * 0.01);
  delete grexp;
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
  // child operators: one scans the first table, while the other scans the 2nd table. The join will operate on these two
  // tables.
  Operator seq_scan_1 = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), catalog::table_oid_t(1),
                                      std::vector<AnnotatedExpression>(), "table", false);
  Operator seq_scan_2 = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), catalog::table_oid_t(2),
                                      std::vector<AnnotatedExpression>(), "table", false);
  std::vector<std::unique_ptr<OperatorNode>> children = {};
  children.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan_1, {})));
  children.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan_2, {})));
  OperatorNode operator_expression = OperatorNode(inner_nl_join, std::move(children));
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&operator_expression));
  // sets row counts for both tables in the groups
  optimizer_context.GetMemo()
      .GetGroupByID(grexp->GetChildGroupId(0))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1)))->GetNumRows());
  optimizer_context.GetMemo()
      .GetGroupByID(grexp->GetChildGroupId(1))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(2)))->GetNumRows());
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  ASSERT_FLOAT_EQ(cost, 0.5);
  delete grexp;
  delete expr_b_1;
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
  OperatorNode operator_expression = OperatorNode(left_nl_join, {});
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&operator_expression));
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  ASSERT_FLOAT_EQ(cost, 0.f);
  delete grexp;
  delete expr_b_1;
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
  OperatorNode operator_expression = OperatorNode(right_nl_join, {});
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&operator_expression));
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  ASSERT_FLOAT_EQ(cost, 0.f);
  delete grexp;
  delete expr_b_1;
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
  OperatorNode operator_expression = OperatorNode(outer_nl_join, {});
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&operator_expression));
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  ASSERT_FLOAT_EQ(cost, 0.f);
  delete grexp;
  delete expr_b_1;
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
  OperatorNode operator_expression = OperatorNode(left_hash_join, {});
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&operator_expression));
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  ASSERT_FLOAT_EQ(cost, 0.f);
  delete grexp;
  delete expr_b_1;
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
  OperatorNode operator_expression = OperatorNode(right_hash_join, {});
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&operator_expression));
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  ASSERT_FLOAT_EQ(cost, 0.f);
  delete grexp;
  delete expr_b_1;
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
  OperatorNode operator_expression = OperatorNode(outer_hash_join, {});
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&operator_expression));
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  ASSERT_FLOAT_EQ(cost, 0.f);
  delete expr_b_1;
  delete grexp;
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
  OperatorNode operator_expression = OperatorNode(insert, {});
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&operator_expression));
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  ASSERT_FLOAT_EQ(cost, 0.f);
  delete grexp;
}

// NOLINTNEXTLINE
TEST_F(DefaultCostModelTests, InsertSelectTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&default_cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  std::vector<catalog::index_oid_t> index_oids{1};
  Operator insert_select = InsertSelect::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1),
                                              catalog::table_oid_t(1), std::vector<catalog::index_oid_t>(index_oids));
  OperatorNode operator_expression = OperatorNode(insert_select, {});
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&operator_expression));
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  ASSERT_FLOAT_EQ(cost, 0.f);
  delete grexp;
}

// NOLINTNEXTLINE
TEST_F(DefaultCostModelTests, DeleteTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&default_cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  Operator delete_op =
      Delete::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), "table", catalog::table_oid_t(1));
  OperatorNode operator_expression = OperatorNode(delete_op, {});
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&operator_expression));
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  ASSERT_FLOAT_EQ(cost, 0.f);
  delete grexp;
}

// NOLINTNEXTLINE
TEST_F(DefaultCostModelTests, UpdateTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&default_cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  Operator update =
      Update::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), "table", catalog::table_oid_t(1), {});
  OperatorNode operator_expression = OperatorNode(update, {});
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&operator_expression));
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  ASSERT_FLOAT_EQ(cost, 0.f);
  delete grexp;
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
  std::vector<std::unique_ptr<OperatorNode>> children = {};
  children.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan, {})));
  OperatorNode operator_expression = OperatorNode(hash_group_by, std::move(children));
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&operator_expression));
  optimizer_context.GetMemo()
      .GetGroupByID(grexp->GetChildGroupId(0))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1)))->GetNumRows());
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  ASSERT_FLOAT_EQ(cost, 10 * 0.01);
  delete grexp;
  delete expr_b_1;
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
  std::vector<std::unique_ptr<OperatorNode>> children = {};
  children.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan, {})));
  OperatorNode operator_expression = OperatorNode(sort_group_by, std::move(children));
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&operator_expression));
  optimizer_context.GetMemo()
      .GetGroupByID(grexp->GetChildGroupId(0))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1)))->GetNumRows());
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  ASSERT_FLOAT_EQ(cost, 5 * 0.01);
  delete grexp;
  delete expr_b_1;
}

// NOLINTNEXTLINE
TEST_F(DefaultCostModelTests, AggregateTest) {
  OptimizerContext optimizer_context =
      OptimizerContext(common::ManagedPointer<AbstractCostModel>(&default_cost_model_));
  optimizer_context.SetStatsStorage(&stats_storage_);
  Operator aggregate = Aggregate::Make();
  Operator seq_scan = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(1),
                                    std::vector<AnnotatedExpression>(), "table", false);
  std::vector<std::unique_ptr<OperatorNode>> children = {};
  children.push_back(std::make_unique<OperatorNode>(OperatorNode(seq_scan, {})));
  OperatorNode operator_expression = OperatorNode(aggregate, std::move(children));
  GroupExpression *grexp =
      optimizer_context.MakeGroupExpression(common::ManagedPointer<OperatorNode>(&operator_expression));
  optimizer_context.GetMemo()
      .GetGroupByID(grexp->GetChildGroupId(0))
      ->SetNumRows((stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1)))->GetNumRows());
  auto cost = default_cost_model_.CalculateCost(optimizer_context.GetTxn(), &optimizer_context.GetMemo(), grexp);
  ASSERT_FLOAT_EQ(cost, 10 * 0.01);
  delete grexp;
}

}  // namespace terrier::optimizer
