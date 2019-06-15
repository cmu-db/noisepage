#include <unordered_map>
#include <unordered_set>
#include <string>
#include <vector>
#include <utility>

#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/group_expression.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/update_statement.h"
#include "transaction/transaction_context.h"
#include "type/transient_value.h"
#include "type/transient_value_factory.h"

#include "gtest/gtest.h"

namespace terrier::optimizer {
// NOLINTNEXTLINE
TEST(TrivialCostModelTests, SeqScanTest) {
  Operator seq_scan = SeqScan::make(catalog::db_oid_t(2), catalog::namespace_oid_t(2), catalog::table_oid_t(3),
                                    std::vector<AnnotatedExpression>(), "table", false);
  GroupExpression g = GroupExpression(seq_scan);
  GroupExpression *gexpr = &g;
  Memo *memo = nullptr;
  transaction::TransactionContext *txn = nullptr;

  auto cost_model = TrivialCostModel();

  double output_cost = cost_model.CalculateCost(gexpr, memo, txn);
  double expected_output_cost = 1.f;

  EXPECT_EQ(output_cost, expected_output_cost);
}

// NOLINTNEXTLINE
TEST(TrivialCostModelTests, IndexScanTest) {
  Operator index_scan =
      IndexScan::make(catalog::db_oid_t(2), catalog::namespace_oid_t(2), catalog::index_oid_t(3),
                      std::vector<AnnotatedExpression>(), "table", false, std::vector<catalog::col_oid_t>(),
                      std::vector<parser::ExpressionType>(), std::vector<type::TransientValue>());
  GroupExpression g = GroupExpression(index_scan);
  GroupExpression *gexpr = &g;
  Memo *memo = nullptr;
  transaction::TransactionContext *txn = nullptr;

  auto cost_model = TrivialCostModel();

  double output_cost = cost_model.CalculateCost(gexpr, memo, txn);
  double expected_output_cost = 0.f;

  EXPECT_EQ(output_cost, expected_output_cost);
}

// NOLINTNEXTLINE
TEST(TrivialCostModelTests, QueryDerivedScanTest) {
  auto alias_to_expr_map = std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>>();

  Operator query_derived_scan = QueryDerivedScan::make("alias", std::move(alias_to_expr_map));
  GroupExpression g = GroupExpression(query_derived_scan);
  GroupExpression *gexpr = &g;
  Memo *memo = nullptr;
  transaction::TransactionContext *txn = nullptr;

  auto cost_model = TrivialCostModel();

  double output_cost = cost_model.CalculateCost(gexpr, memo, txn);
  double expected_output_cost = 0.f;

  EXPECT_EQ(output_cost, expected_output_cost);
}

// NOLINTNEXTLINE
TEST(TrivialCostModelTests, OrderByTest) {
  Operator order_by = OrderBy::make();
  GroupExpression g = GroupExpression(order_by);
  GroupExpression *gexpr = &g;
  Memo *memo = nullptr;
  transaction::TransactionContext *txn = nullptr;

  auto cost_model = TrivialCostModel();

  double output_cost = cost_model.CalculateCost(gexpr, memo, txn);
  double expected_output_cost = 0.f;

  EXPECT_EQ(output_cost, expected_output_cost);
}

// NOLINTNEXTLINE
TEST(TrivialCostModelTests, LimitTest) {
  size_t offset = 90;
  size_t limit = 22;
  auto sort_expr_ori = new parser::ConstantValueExpression(type::TransientValueFactory::GetTinyInt(1));
  auto sort_expr = common::ManagedPointer<parser::AbstractExpression>(sort_expr_ori);
  planner::OrderByOrderingType sort_dir = planner::OrderByOrderingType::ASC;

  Operator lim = Limit::make(offset, limit, {sort_expr}, {sort_dir});
  GroupExpression g = GroupExpression(lim);
  GroupExpression *gexpr = &g;
  Memo *memo = nullptr;
  transaction::TransactionContext *txn = nullptr;

  auto cost_model = TrivialCostModel();

  double output_cost = cost_model.CalculateCost(gexpr, memo, txn);
  double expected_output_cost = 0.f;

  EXPECT_EQ(output_cost, expected_output_cost);

  delete sort_expr_ori;
}

// NOLINTNEXTLINE
TEST(TrivialCostModelTests, InnerNLJoinTest) {
  auto expr_b = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));

  auto x = common::ManagedPointer<parser::AbstractExpression>(expr_b);

  Operator inner_nl_join = InnerNLJoin::make(std::vector<AnnotatedExpression>(), {x}, {x});

  GroupExpression g = GroupExpression(inner_nl_join);
  GroupExpression *gexpr = &g;
  Memo *memo = nullptr;
  transaction::TransactionContext *txn = nullptr;

  auto cost_model = TrivialCostModel();

  double output_cost = cost_model.CalculateCost(gexpr, memo, txn);
  double expected_output_cost = 0.f;

  EXPECT_EQ(output_cost, expected_output_cost);

  delete expr_b;
}

// NOLINTNEXTLINE
TEST(TrivialCostModelTests, LeftNLJoinTest) {
  auto expr_b = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));

  auto x = common::ManagedPointer<parser::AbstractExpression>(expr_b);

  Operator left_nl_join = LeftNLJoin::make(x);

  GroupExpression g = GroupExpression(left_nl_join);
  GroupExpression *gexpr = &g;
  Memo *memo = nullptr;
  transaction::TransactionContext *txn = nullptr;

  auto cost_model = TrivialCostModel();

  double output_cost = cost_model.CalculateCost(gexpr, memo, txn);
  double expected_output_cost = 0;

  EXPECT_EQ(output_cost, expected_output_cost);

  delete expr_b;
}

// NOLINTNEXTLINE
TEST(TrivialCostModelTests, RightNLJoinTest) {
  auto expr_b = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));

  auto x = common::ManagedPointer<parser::AbstractExpression>(expr_b);

  Operator right_nl_join = RightNLJoin::make(x);

  GroupExpression g = GroupExpression(right_nl_join);
  GroupExpression *gexpr = &g;
  Memo *memo = nullptr;
  transaction::TransactionContext *txn = nullptr;

  auto cost_model = TrivialCostModel();

  double output_cost = cost_model.CalculateCost(gexpr, memo, txn);
  double expected_output_cost = 0;

  EXPECT_EQ(output_cost, expected_output_cost);

  delete expr_b;
}

// NOLINTNEXTLINE
TEST(TrivialCostModelTests, OuterNLJoinTest) {
  auto expr_b = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));

  auto x = common::ManagedPointer<parser::AbstractExpression>(expr_b);

  Operator outer_nl_join = OuterNLJoin::make(x);

  GroupExpression g = GroupExpression(outer_nl_join);
  GroupExpression *gexpr = &g;
  Memo *memo = nullptr;
  transaction::TransactionContext *txn = nullptr;

  auto cost_model = TrivialCostModel();

  double output_cost = cost_model.CalculateCost(gexpr, memo, txn);
  double expected_output_cost = 0;

  EXPECT_EQ(output_cost, expected_output_cost);

  delete expr_b;
}

// NOLINTNEXTLINE
TEST(TrivialCostModelTests, InnerHashJoinTest) {
  auto expr_b = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));

  auto x = common::ManagedPointer<parser::AbstractExpression>(expr_b);

  Operator inner_hash_join = InnerHashJoin::make(std::vector<AnnotatedExpression>(), {x}, {x});

  GroupExpression g = GroupExpression(inner_hash_join);
  GroupExpression *gexpr = &g;
  Memo *memo = nullptr;
  transaction::TransactionContext *txn = nullptr;

  auto cost_model = TrivialCostModel();

  double output_cost = cost_model.CalculateCost(gexpr, memo, txn);
  double expected_output_cost = 1.f;

  EXPECT_EQ(output_cost, expected_output_cost);

  delete expr_b;
}

// NOLINTNEXTLINE
TEST(TrivialCostModelTests, LeftHashJoinTest) {
  auto expr_b = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));

  auto x = common::ManagedPointer<parser::AbstractExpression>(expr_b);

  Operator left_hash_join = LeftHashJoin::make(x);

  GroupExpression g = GroupExpression(left_hash_join);
  GroupExpression *gexpr = &g;
  Memo *memo = nullptr;
  transaction::TransactionContext *txn = nullptr;

  auto cost_model = TrivialCostModel();

  double output_cost = cost_model.CalculateCost(gexpr, memo, txn);
  double expected_output_cost = 0;

  EXPECT_EQ(output_cost, expected_output_cost);

  delete expr_b;
}

// NOLINTNEXTLINE
TEST(TrivialCostModelTests, RightHashJoinTest) {
  auto expr_b = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));

  auto x = common::ManagedPointer<parser::AbstractExpression>(expr_b);

  Operator right_hash_join = RightHashJoin::make(x);

  GroupExpression g = GroupExpression(right_hash_join);
  GroupExpression *gexpr = &g;
  Memo *memo = nullptr;
  transaction::TransactionContext *txn = nullptr;

  auto cost_model = TrivialCostModel();

  double output_cost = cost_model.CalculateCost(gexpr, memo, txn);
  double expected_output_cost = 0;

  EXPECT_EQ(output_cost, expected_output_cost);

  delete expr_b;
}

// NOLINTNEXTLINE
TEST(TrivialCostModelTests, OuterHashJoinTest) {
  auto expr_b = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));

  auto x = common::ManagedPointer<parser::AbstractExpression>(expr_b);

  Operator outer_hash_join = OuterHashJoin::make(x);

  GroupExpression g = GroupExpression(outer_hash_join);
  GroupExpression *gexpr = &g;
  Memo *memo = nullptr;
  transaction::TransactionContext *txn = nullptr;

  auto cost_model = TrivialCostModel();

  double output_cost = cost_model.CalculateCost(gexpr, memo, txn);
  double expected_output_cost = 0;

  EXPECT_EQ(output_cost, expected_output_cost);

  delete expr_b;
}

// NOLINTNEXTLINE
TEST(TrivialCostModelTests, InsertTest) {
  catalog::db_oid_t database_oid(123);
  catalog::namespace_oid_t namespace_oid(456);
  catalog::table_oid_t table_oid(789);
  catalog::col_oid_t columns[] = {catalog::col_oid_t(1), catalog::col_oid_t(2)};
  parser::AbstractExpression *raw_values[] = {
      new parser::ConstantValueExpression(type::TransientValueFactory::GetTinyInt(1)),
      new parser::ConstantValueExpression(type::TransientValueFactory::GetTinyInt(9))};
  std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>> values = {
      std::vector<common::ManagedPointer<parser::AbstractExpression>>(raw_values, std::end(raw_values))};

  Operator insert =
      Insert::make(database_oid, namespace_oid, table_oid, std::vector<catalog::col_oid_t>(columns, std::end(columns)),
                   std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>>(values));

  GroupExpression g = GroupExpression(insert);
  GroupExpression *gexpr = &g;
  Memo *memo = nullptr;
  transaction::TransactionContext *txn = nullptr;

  auto cost_model = TrivialCostModel();

  double output_cost = cost_model.CalculateCost(gexpr, memo, txn);
  double expected_output_cost = 0;

  EXPECT_EQ(output_cost, expected_output_cost);
}

// NOLINTNEXTLINE
TEST(TrivialCostModelTests, InsertSelectTest) {
  catalog::db_oid_t database_oid(123);
  catalog::namespace_oid_t namespace_oid(456);
  catalog::table_oid_t table_oid(789);

  Operator insert_select = InsertSelect::make(database_oid, namespace_oid, table_oid);

  GroupExpression g = GroupExpression(insert_select);
  GroupExpression *gexpr = &g;
  Memo *memo = nullptr;
  transaction::TransactionContext *txn = nullptr;

  auto cost_model = TrivialCostModel();

  double output_cost = cost_model.CalculateCost(gexpr, memo, txn);
  double expected_output_cost = 0;

  EXPECT_EQ(output_cost, expected_output_cost);
}

// NOLINTNEXTLINE
TEST(TrivialCostModelTests, DeleteTest) {
  auto expr_b = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));

  auto x = common::ManagedPointer<parser::AbstractExpression>(expr_b);

  catalog::db_oid_t database_oid(123);
  catalog::namespace_oid_t namespace_oid(456);
  catalog::table_oid_t table_oid(789);

  Operator del = Delete::make(database_oid, namespace_oid, table_oid, x);

  GroupExpression g = GroupExpression(del);
  GroupExpression *gexpr = &g;
  Memo *memo = nullptr;
  transaction::TransactionContext *txn = nullptr;

  auto cost_model = TrivialCostModel();

  double output_cost = cost_model.CalculateCost(gexpr, memo, txn);
  double expected_output_cost = 0;

  EXPECT_EQ(output_cost, expected_output_cost);

  delete expr_b;
}

// NOLINTNEXTLINE
TEST(TrivialCostModelTests, UpdateTest) {
  std::string column = "abc";
  parser::AbstractExpression *value = new parser::ConstantValueExpression(type::TransientValueFactory::GetTinyInt(1));

  auto raw_update_clause = new parser::UpdateClause(column, common::ManagedPointer<parser::AbstractExpression>(value));
  auto update_clause = common::ManagedPointer(raw_update_clause);

  catalog::db_oid_t database_oid(123);
  catalog::namespace_oid_t namespace_oid(456);
  catalog::table_oid_t table_oid(789);

  Operator update = Update::make(database_oid, namespace_oid, table_oid, {update_clause});

  GroupExpression g = GroupExpression(update);
  GroupExpression *gexpr = &g;
  Memo *memo = nullptr;
  transaction::TransactionContext *txn = nullptr;

  auto cost_model = TrivialCostModel();

  double output_cost = cost_model.CalculateCost(gexpr, memo, txn);
  double expected_output_cost = 0;

  EXPECT_EQ(output_cost, expected_output_cost);

  delete value;
  delete raw_update_clause;
}

// NOLINTNEXTLINE
TEST(TrivialCostModelTests, HashGroupByTest) {
  auto expr_b = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));

  auto x = common::ManagedPointer<parser::AbstractExpression>(expr_b);

  auto annotated_expr =
      AnnotatedExpression(common::ManagedPointer<parser::AbstractExpression>(), std::unordered_set<std::string>());

  Operator group_by = HashGroupBy::make(std::vector<common::ManagedPointer<parser::AbstractExpression>>{x},
                                        std::vector<AnnotatedExpression>{annotated_expr});

  GroupExpression g = GroupExpression(group_by);
  GroupExpression *gexpr = &g;
  Memo *memo = nullptr;
  transaction::TransactionContext *txn = nullptr;

  auto cost_model = TrivialCostModel();

  double output_cost = cost_model.CalculateCost(gexpr, memo, txn);
  double expected_output_cost = 0.f;

  EXPECT_EQ(output_cost, expected_output_cost);

  delete expr_b;
}

// NOLINTNEXTLINE
TEST(TrivialCostModelTests, SortGroupByTest) {
  auto expr_b = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));

  auto x = common::ManagedPointer<parser::AbstractExpression>(expr_b);

  auto annotated_expr =
      AnnotatedExpression(common::ManagedPointer<parser::AbstractExpression>(), std::unordered_set<std::string>());

  Operator sort_group_by = SortGroupBy::make(std::vector<common::ManagedPointer<parser::AbstractExpression>>{x},
                                             std::vector<AnnotatedExpression>{annotated_expr});

  GroupExpression g = GroupExpression(sort_group_by);
  GroupExpression *gexpr = &g;
  Memo *memo = nullptr;
  transaction::TransactionContext *txn = nullptr;

  auto cost_model = TrivialCostModel();

  double output_cost = cost_model.CalculateCost(gexpr, memo, txn);
  double expected_output_cost = 1.f;

  EXPECT_EQ(output_cost, expected_output_cost);

  delete expr_b;
}

// NOLINTNEXTLINE
TEST(TrivialCostModelTests, DistinctTest) {
  Operator distinct = Distinct::make();

  GroupExpression g = GroupExpression(distinct);
  GroupExpression *gexpr = &g;
  Memo *memo = nullptr;
  transaction::TransactionContext *txn = nullptr;

  auto cost_model = TrivialCostModel();

  double output_cost = cost_model.CalculateCost(gexpr, memo, txn);
  double expected_output_cost = 0.f;

  EXPECT_EQ(output_cost, expected_output_cost);
}

// NOLINTNEXTLINE
TEST(TrivialCostModelTests, AggregateTest) {
  Operator aggregate = Aggregate::make();

  GroupExpression g = GroupExpression(aggregate);
  GroupExpression *gexpr = &g;
  Memo *memo = nullptr;
  transaction::TransactionContext *txn = nullptr;

  auto cost_model = TrivialCostModel();

  double output_cost = cost_model.CalculateCost(gexpr, memo, txn);
  double expected_output_cost = 0.f;

  EXPECT_EQ(output_cost, expected_output_cost);
}

}  // namespace terrier::optimizer
