#include "optimizer/statistics/stats_calculator.h"

#include "gtest/gtest.h"
#include "optimizer/logical_operators.h"
#include "optimizer/optimizer_context.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/comparison_expression.h"
#include "parser/expression/conjunction_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression/operator_expression.h"
#include "test_util/end_to_end_test.h"
#include "test_util/test_harness.h"

namespace noisepage::optimizer {
class StatsCalculatorTests : public test::EndToEndTest {
 protected:
  StatsCalculator stats_calculator_;

  OptimizerContext context_{nullptr};
  std::string table_name_1_ = "empty_nullable_table";
  std::string table_1_col_1_name_ = "colA";
  catalog::table_oid_t table_oid_1_;
  catalog::col_oid_t table_1_col_oid_;

  std::string table_name_2_ = "empty_table2";
  std::string table_2_col_1_name_ = "colA";
  std::string table_2_col_2_name_ = "colB";
  catalog::table_oid_t table_oid_2_;
  catalog::col_oid_t table_2_col_1_oid_;
  catalog::col_oid_t table_2_col_2_oid_;

 public:
  void SetUp() override {
    EndToEndTest::SetUp();
    auto exec_ctx = MakeExecCtx();
    GenerateTestTables(exec_ctx.get());

    context_.SetStatsStorage(stats_storage_.Get());
    context_.SetCatalogAccessor(accessor_.get());

    table_oid_1_ = accessor_->GetTableOid(table_name_1_);
    table_oid_2_ = accessor_->GetTableOid(table_name_2_);

    NOISEPAGE_ASSERT(accessor_->GetSchema(table_oid_1_).GetColumns().size() == 1, "Table should have 1 column");
    table_1_col_oid_ = accessor_->GetSchema(table_oid_1_).GetColumns().at(0).Oid();

    NOISEPAGE_ASSERT(accessor_->GetSchema(table_oid_2_).GetColumns().size() == 2, "Table should have 2 column");
    table_2_col_1_oid_ = accessor_->GetSchema(table_oid_2_).GetColumns().at(0).Oid();
    table_2_col_2_oid_ = accessor_->GetSchema(table_oid_2_).GetColumns().at(1).Oid();
  }
};

// NOLINTNEXTLINE
TEST_F(StatsCalculatorTests, TestLogicalGet) {
  RunQuery("INSERT INTO " + table_name_1_ + " VALUES(1), (NULL), (3);");
  RunQuery("ANALYZE " + table_name_1_ + ";");
  txn_manager_->Commit(test_txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
  test_txn_ = txn_manager_->BeginTransaction();

  // Constructing logical get with no predicates from "empty_nullable_table"
  Operator logical_get =
      LogicalGet::Make(test_db_oid_, table_oid_1_, {}, table_name_1_, false).RegisterWithTxnContext(test_txn_);
  GroupExpression *gexpr = new GroupExpression(logical_get, {}, test_txn_);
  gexpr->SetGroupID(group_id_t(1));
  context_.GetMemo().InsertExpression(gexpr, false);

  // CVE for column 1
  parser::ColumnValueExpression col_a(table_name_1_, table_1_col_1_name_, test_db_oid_, table_oid_1_, table_1_col_oid_,
                                      type::TypeId::INTEGER);
  ExprSet required_cols;
  required_cols.emplace(&col_a);

  stats_calculator_.CalculateStats(gexpr, &context_);

  auto *root_group = context_.GetMemo().GetGroupByID(gexpr->GetGroupID());
  EXPECT_EQ(root_group->GetNumRows(), 3);
  EXPECT_TRUE(root_group->HasNumRows());
}

// NOLINTNEXTLINE
TEST_F(StatsCalculatorTests, TestInvalidLogicalGet) {
  // Constructing logical get with no predicates from invalid table
  Operator logical_get =
      LogicalGet::Make(test_db_oid_, catalog::INVALID_TABLE_OID, {}, "", false).RegisterWithTxnContext(test_txn_);
  GroupExpression *gexpr = new GroupExpression(logical_get, {}, test_txn_);
  gexpr->SetGroupID(group_id_t(1));
  context_.GetMemo().InsertExpression(gexpr, false);

  // CVE for column 1
  parser::ColumnValueExpression col_a(table_name_1_, table_1_col_1_name_, test_db_oid_, table_oid_1_, table_1_col_oid_,
                                      type::TypeId::INTEGER);
  ExprSet required_cols;
  required_cols.emplace(&col_a);

  stats_calculator_.CalculateStats(gexpr, &context_);

  auto *root_group = context_.GetMemo().GetGroupByID(gexpr->GetGroupID());
  EXPECT_EQ(root_group->GetNumRows(), -1);
  EXPECT_FALSE(root_group->HasNumRows());
}

// NOLINTNEXTLINE
TEST_F(StatsCalculatorTests, TestNotPredicate) {
  RunQuery("INSERT INTO " + table_name_1_ + " VALUES(1), (NULL), (3);");
  RunQuery("ANALYZE " + table_name_1_ + ";");
  txn_manager_->Commit(test_txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
  test_txn_ = txn_manager_->BeginTransaction();

  // Constructing Logical Get with NOT EQUALS predicate "NOT colA = 1" from "empty_nullable_table"
  parser::ColumnValueExpression col_a(table_name_1_, table_1_col_1_name_, test_db_oid_, table_oid_1_, table_1_col_oid_,
                                      type::TypeId::INTEGER);
  auto one = std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(1));
  std::vector<std::unique_ptr<parser::AbstractExpression>> equal_child_exprs;
  equal_child_exprs.emplace_back(col_a.Copy());
  equal_child_exprs.emplace_back(std::move(one));
  parser::ComparisonExpression equals(parser::ExpressionType::COMPARE_EQUAL, std::move(equal_child_exprs));
  std::vector<std::unique_ptr<parser::AbstractExpression>> not_child_exprs;
  not_child_exprs.emplace_back(equals.Copy());
  parser::OperatorExpression not_op(parser::ExpressionType::OPERATOR_NOT, type::TypeId::BOOLEAN,
                                    std::move(not_child_exprs));
  common::ManagedPointer<parser::AbstractExpression> not_expr(&not_op);
  AnnotatedExpression annotated_not(not_expr, {});

  Operator logical_get = LogicalGet::Make(test_db_oid_, table_oid_1_, {annotated_not}, table_name_1_, false)
                             .RegisterWithTxnContext(test_txn_);
  GroupExpression *gexpr = new GroupExpression(logical_get, {}, test_txn_);
  gexpr->SetGroupID(group_id_t(1));
  context_.GetMemo().InsertExpression(gexpr, false);

  ExprSet required_cols;
  required_cols.emplace(&col_a);

  stats_calculator_.CalculateStats(gexpr, &context_);

  auto *root_group = context_.GetMemo().GetGroupByID(gexpr->GetGroupID());
  EXPECT_EQ(root_group->GetNumRows(), 2);
  EXPECT_TRUE(root_group->HasNumRows());
}

// NOLINTNEXTLINE
TEST_F(StatsCalculatorTests, TestUnaryOperatorPredicate) {
  RunQuery("INSERT INTO " + table_name_1_ + " VALUES(1), (NULL), (3);");
  RunQuery("ANALYZE " + table_name_1_ + ";");
  txn_manager_->Commit(test_txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
  test_txn_ = txn_manager_->BeginTransaction();

  // Constructing Logical Get with IS NOT NULL predicate "colA IS NOT NULL" from "empty_nullable_table"
  parser::ColumnValueExpression col_a(table_name_1_, table_1_col_1_name_, test_db_oid_, table_oid_1_, table_1_col_oid_,
                                      type::TypeId::INTEGER);
  std::vector<std::unique_ptr<parser::AbstractExpression>> not_null_child_exprs;
  not_null_child_exprs.emplace_back(col_a.Copy());
  parser::OperatorExpression not_null_op(parser::ExpressionType::OPERATOR_IS_NOT_NULL, type::TypeId::BOOLEAN,
                                         std::move(not_null_child_exprs));
  common::ManagedPointer<parser::AbstractExpression> not_null_expr(&not_null_op);
  AnnotatedExpression annotated_not_null(not_null_expr, {});

  Operator logical_get = LogicalGet::Make(test_db_oid_, table_oid_1_, {annotated_not_null}, table_name_1_, false)
                             .RegisterWithTxnContext(test_txn_);
  GroupExpression *gexpr = new GroupExpression(logical_get, {}, test_txn_);
  gexpr->SetGroupID(group_id_t(1));
  context_.GetMemo().InsertExpression(gexpr, false);

  ExprSet required_cols;
  required_cols.emplace(&col_a);

  stats_calculator_.CalculateStats(gexpr, &context_);

  auto *root_group = context_.GetMemo().GetGroupByID(gexpr->GetGroupID());
  EXPECT_EQ(root_group->GetNumRows(), 2);
  EXPECT_TRUE(root_group->HasNumRows());
}

// NOLINTNEXTLINE
TEST_F(StatsCalculatorTests, TestLeftSidePredicate) {
  RunQuery("INSERT INTO " + table_name_1_ + " VALUES(1), (NULL), (3);");
  RunQuery("ANALYZE " + table_name_1_ + ";");
  txn_manager_->Commit(test_txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
  test_txn_ = txn_manager_->BeginTransaction();

  // Constructing Logical Get with predicate with column value on the left side of predicate "colA = 1" from
  // "empty_nullable_table"
  parser::ColumnValueExpression col_a(table_name_1_, table_1_col_1_name_, test_db_oid_, table_oid_1_, table_1_col_oid_,
                                      type::TypeId::INTEGER);
  auto one = std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(1));
  std::vector<std::unique_ptr<parser::AbstractExpression>> equal_child_exprs;
  equal_child_exprs.emplace_back(col_a.Copy());
  equal_child_exprs.emplace_back(std::move(one));
  parser::ComparisonExpression equals(parser::ExpressionType::COMPARE_EQUAL, std::move(equal_child_exprs));
  common::ManagedPointer<parser::AbstractExpression> equal_expr(&equals);
  AnnotatedExpression annotated_equals(equal_expr, {});

  Operator logical_get = LogicalGet::Make(test_db_oid_, table_oid_1_, {annotated_equals}, table_name_1_, false)
                             .RegisterWithTxnContext(test_txn_);
  GroupExpression *gexpr = new GroupExpression(logical_get, {}, test_txn_);
  gexpr->SetGroupID(group_id_t(1));
  context_.GetMemo().InsertExpression(gexpr, false);

  ExprSet required_cols;
  required_cols.emplace(&col_a);

  stats_calculator_.CalculateStats(gexpr, &context_);

  auto *root_group = context_.GetMemo().GetGroupByID(gexpr->GetGroupID());
  EXPECT_EQ(root_group->GetNumRows(), 1);
  EXPECT_TRUE(root_group->HasNumRows());
}

// NOLINTNEXTLINE
TEST_F(StatsCalculatorTests, TestLeftSideParamPredicate) {
  RunQuery("INSERT INTO " + table_name_1_ + " VALUES(1), (NULL), (3);");
  RunQuery("ANALYZE " + table_name_1_ + ";");
  txn_manager_->Commit(test_txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
  test_txn_ = txn_manager_->BeginTransaction();

  // Constructing Logical Get with predicate with column value on the left side of predicate and param on the right side
  // of predicate "colA = param" from "empty_nullable_table". (param is 1)
  parser::ColumnValueExpression col_a(table_name_1_, table_1_col_1_name_, test_db_oid_, table_oid_1_, table_1_col_oid_,
                                      type::TypeId::INTEGER);
  auto param = std::make_unique<parser::ParameterValueExpression>(0, type::TypeId::INTEGER);
  std::vector<std::unique_ptr<parser::AbstractExpression>> equal_child_exprs;
  equal_child_exprs.emplace_back(col_a.Copy());
  equal_child_exprs.emplace_back(std::move(param));
  parser::ComparisonExpression equals(parser::ExpressionType::COMPARE_EQUAL, std::move(equal_child_exprs));
  common::ManagedPointer<parser::AbstractExpression> equal_expr(&equals);
  AnnotatedExpression annotated_equals(equal_expr, {});

  Operator logical_get = LogicalGet::Make(test_db_oid_, table_oid_1_, {annotated_equals}, table_name_1_, false)
                             .RegisterWithTxnContext(test_txn_);
  GroupExpression *gexpr = new GroupExpression(logical_get, {}, test_txn_);
  gexpr->SetGroupID(group_id_t(1));
  context_.GetMemo().InsertExpression(gexpr, false);

  parser::ConstantValueExpression one(type::TypeId::INTEGER, execution::sql::Integer(1));
  std::vector<parser::ConstantValueExpression> params;
  params.emplace_back(one);
  context_.SetParams(common::ManagedPointer(&params));

  ExprSet required_cols;
  required_cols.emplace(&col_a);

  stats_calculator_.CalculateStats(gexpr, &context_);

  auto *root_group = context_.GetMemo().GetGroupByID(gexpr->GetGroupID());
  EXPECT_EQ(root_group->GetNumRows(), 1);
  EXPECT_TRUE(root_group->HasNumRows());
}

// NOLINTNEXTLINE
TEST_F(StatsCalculatorTests, TestRightSidePredicate) {
  RunQuery("INSERT INTO " + table_name_1_ + " VALUES(1), (NULL), (3);");
  RunQuery("ANALYZE " + table_name_1_ + ";");
  txn_manager_->Commit(test_txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
  test_txn_ = txn_manager_->BeginTransaction();

  // Constructing Logical Get with predicate with column value on the right side of predicate "3 = colA" from
  // "empty_nullable_table"
  parser::ColumnValueExpression col_a(table_name_1_, table_1_col_1_name_, test_db_oid_, table_oid_1_, table_1_col_oid_,
                                      type::TypeId::INTEGER);
  auto three = std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(3));
  std::vector<std::unique_ptr<parser::AbstractExpression>> equal_child_exprs;
  equal_child_exprs.emplace_back(std::move(three));
  equal_child_exprs.emplace_back(col_a.Copy());
  parser::ComparisonExpression equals(parser::ExpressionType::COMPARE_EQUAL, std::move(equal_child_exprs));
  common::ManagedPointer<parser::AbstractExpression> equal_expr(&equals);
  AnnotatedExpression annotated_equals(equal_expr, {});

  Operator logical_get = LogicalGet::Make(test_db_oid_, table_oid_1_, {annotated_equals}, table_name_1_, false)
                             .RegisterWithTxnContext(test_txn_);
  GroupExpression *gexpr = new GroupExpression(logical_get, {}, test_txn_);
  gexpr->SetGroupID(group_id_t(1));
  context_.GetMemo().InsertExpression(gexpr, false);

  ExprSet required_cols;
  required_cols.emplace(&col_a);

  stats_calculator_.CalculateStats(gexpr, &context_);

  auto *root_group = context_.GetMemo().GetGroupByID(gexpr->GetGroupID());
  EXPECT_EQ(root_group->GetNumRows(), 1);
  EXPECT_TRUE(root_group->HasNumRows());
}

// NOLINTNEXTLINE
TEST_F(StatsCalculatorTests, TestRightSideParamPredicate) {
  RunQuery("INSERT INTO " + table_name_1_ + " VALUES(1), (NULL), (3);");
  RunQuery("ANALYZE " + table_name_1_ + ";");
  txn_manager_->Commit(test_txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
  test_txn_ = txn_manager_->BeginTransaction();

  // Constructing Logical Get with predicate with column value on the right side of predicate and param on the left side
  // of predicate "param = colA" from "empty_nullable_table". (param is 3)
  parser::ColumnValueExpression col_a(table_name_1_, table_1_col_1_name_, test_db_oid_, table_oid_1_, table_1_col_oid_,
                                      type::TypeId::INTEGER);
  auto param = std::make_unique<parser::ParameterValueExpression>(0, type::TypeId::INTEGER);
  std::vector<std::unique_ptr<parser::AbstractExpression>> equal_child_exprs;
  equal_child_exprs.emplace_back(std::move(param));
  equal_child_exprs.emplace_back(col_a.Copy());
  parser::ComparisonExpression equals(parser::ExpressionType::COMPARE_EQUAL, std::move(equal_child_exprs));
  common::ManagedPointer<parser::AbstractExpression> equal_expr(&equals);
  AnnotatedExpression annotated_equals(equal_expr, {});

  Operator logical_get = LogicalGet::Make(test_db_oid_, table_oid_1_, {annotated_equals}, table_name_1_, false)
                             .RegisterWithTxnContext(test_txn_);
  GroupExpression *gexpr = new GroupExpression(logical_get, {}, test_txn_);
  gexpr->SetGroupID(group_id_t(1));
  context_.GetMemo().InsertExpression(gexpr, false);

  parser::ConstantValueExpression three(type::TypeId::INTEGER, execution::sql::Integer(3));
  std::vector<parser::ConstantValueExpression> params;
  params.emplace_back(three);
  context_.SetParams(common::ManagedPointer(&params));

  ExprSet required_cols;
  required_cols.emplace(&col_a);

  stats_calculator_.CalculateStats(gexpr, &context_);

  auto *root_group = context_.GetMemo().GetGroupByID(gexpr->GetGroupID());
  EXPECT_EQ(root_group->GetNumRows(), 1);
  EXPECT_TRUE(root_group->HasNumRows());
}

// NOLINTNEXTLINE
TEST_F(StatsCalculatorTests, TestMultipleParamPredicate) {
  RunQuery("INSERT INTO " + table_name_2_ + " VALUES(1, TRUE), (NULL, FALSE), (3, FALSE);");
  RunQuery("ANALYZE " + table_name_2_ + ";");
  txn_manager_->Commit(test_txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
  test_txn_ = txn_manager_->BeginTransaction();

  // Constructing Logical Get with two param predicates "calA = param1 AND colB = param2" from "empty_table2".
  // (param1 is 1, param2 is true)
  parser::ColumnValueExpression col_a(table_name_2_, table_2_col_1_name_, test_db_oid_, table_oid_2_,
                                      table_2_col_1_oid_, type::TypeId::INTEGER);
  parser::ColumnValueExpression col_b(table_name_2_, table_2_col_2_name_, test_db_oid_, table_oid_2_,
                                      table_2_col_2_oid_, type::TypeId::INTEGER);
  auto param1 = std::make_unique<parser::ParameterValueExpression>(0, type::TypeId::INTEGER);
  auto param2 = std::make_unique<parser::ParameterValueExpression>(1, type::TypeId::BOOLEAN);

  std::vector<std::unique_ptr<parser::AbstractExpression>> equal1_child_exprs;
  equal1_child_exprs.emplace_back(col_a.Copy());
  equal1_child_exprs.emplace_back(std::move(param1));
  parser::ComparisonExpression equals1(parser::ExpressionType::COMPARE_EQUAL, std::move(equal1_child_exprs));
  common::ManagedPointer<parser::AbstractExpression> equal_expr1(&equals1);
  AnnotatedExpression annotated_equals1(equal_expr1, {});

  std::vector<std::unique_ptr<parser::AbstractExpression>> equal2_child_exprs;
  equal2_child_exprs.emplace_back(col_b.Copy());
  equal2_child_exprs.emplace_back(std::move(param2));
  parser::ComparisonExpression equals2(parser::ExpressionType::COMPARE_EQUAL, std::move(equal2_child_exprs));
  common::ManagedPointer<parser::AbstractExpression> equal_expr2(&equals2);
  AnnotatedExpression annotated_equals2(equal_expr2, {});

  Operator logical_get =
      LogicalGet::Make(test_db_oid_, table_oid_2_, {annotated_equals1, annotated_equals2}, table_name_2_, false)
          .RegisterWithTxnContext(test_txn_);
  GroupExpression *gexpr = new GroupExpression(logical_get, {}, test_txn_);
  gexpr->SetGroupID(group_id_t(1));
  context_.GetMemo().InsertExpression(gexpr, false);

  parser::ConstantValueExpression one(type::TypeId::INTEGER, execution::sql::Integer(1));
  parser::ConstantValueExpression true_val(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  std::vector<parser::ConstantValueExpression> params;
  params.emplace_back(one);
  params.emplace_back(true_val);
  context_.SetParams(common::ManagedPointer(&params));

  ExprSet required_cols;
  required_cols.emplace(&col_a);
  required_cols.emplace(&col_b);

  stats_calculator_.CalculateStats(gexpr, &context_);

  auto *root_group = context_.GetMemo().GetGroupByID(gexpr->GetGroupID());
  // Unfortunately the calculation is not that accurate
  EXPECT_EQ(root_group->GetNumRows(), 0);
  EXPECT_TRUE(root_group->HasNumRows());
}

// NOLINTNEXTLINE
TEST_F(StatsCalculatorTests, TestAndPredicate) {
  RunQuery("INSERT INTO " + table_name_1_ + " VALUES(1), (NULL), (3);");
  RunQuery("ANALYZE " + table_name_1_ + ";");
  txn_manager_->Commit(test_txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
  test_txn_ = txn_manager_->BeginTransaction();

  // Constructing Logical Get with AND predicate consisting of two EQUALS predicates "colA = 3 AND colA = 1"
  // from "empty_nullable_table".
  parser::ColumnValueExpression col_a(table_name_1_, table_1_col_1_name_, test_db_oid_, table_oid_1_, table_1_col_oid_,
                                      type::TypeId::INTEGER);

  auto three = std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(3));
  std::vector<std::unique_ptr<parser::AbstractExpression>> equal_three_child_exprs;
  equal_three_child_exprs.emplace_back(std::move(three));
  equal_three_child_exprs.emplace_back(col_a.Copy());
  parser::ComparisonExpression equals_three(parser::ExpressionType::COMPARE_EQUAL, std::move(equal_three_child_exprs));

  auto one = std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(1));
  std::vector<std::unique_ptr<parser::AbstractExpression>> equal_child_exprs;
  equal_child_exprs.emplace_back(col_a.Copy());
  equal_child_exprs.emplace_back(std::move(one));
  parser::ComparisonExpression equals_one(parser::ExpressionType::COMPARE_EQUAL, std::move(equal_child_exprs));

  std::vector<std::unique_ptr<parser::AbstractExpression>> and_child_exprs;
  and_child_exprs.emplace_back(equals_three.Copy());
  and_child_exprs.emplace_back(equals_one.Copy());
  parser::ConjunctionExpression and_op(parser::ExpressionType::CONJUNCTION_AND, std::move(and_child_exprs));

  common::ManagedPointer<parser::AbstractExpression> and_expr(&and_op);
  AnnotatedExpression annotated_and(and_expr, {});

  Operator logical_get = LogicalGet::Make(test_db_oid_, table_oid_1_, {annotated_and}, table_name_1_, false)
                             .RegisterWithTxnContext(test_txn_);
  GroupExpression *gexpr = new GroupExpression(logical_get, {}, test_txn_);
  gexpr->SetGroupID(group_id_t(1));
  context_.GetMemo().InsertExpression(gexpr, false);

  ExprSet required_cols;
  required_cols.emplace(&col_a);

  stats_calculator_.CalculateStats(gexpr, &context_);

  auto *root_group = context_.GetMemo().GetGroupByID(gexpr->GetGroupID());
  EXPECT_EQ(root_group->GetNumRows(), 0);
  EXPECT_TRUE(root_group->HasNumRows());
}

// NOLINTNEXTLINE
TEST_F(StatsCalculatorTests, TestOrPredicate) {
  RunQuery("INSERT INTO " + table_name_1_ + " VALUES(1), (NULL), (3);");
  RunQuery("ANALYZE " + table_name_1_ + ";");
  txn_manager_->Commit(test_txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
  test_txn_ = txn_manager_->BeginTransaction();

  // Constructing Logical Get with OR predicate consisting of two EQUALS predicates "colA = 3 OR colA = 1"
  // from "empty_nullable_table"
  parser::ColumnValueExpression col_a(table_name_1_, table_1_col_1_name_, test_db_oid_, table_oid_1_, table_1_col_oid_,
                                      type::TypeId::INTEGER);

  auto three = std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(3));
  std::vector<std::unique_ptr<parser::AbstractExpression>> equal_three_child_exprs;
  equal_three_child_exprs.emplace_back(std::move(three));
  equal_three_child_exprs.emplace_back(col_a.Copy());
  parser::ComparisonExpression equals_three(parser::ExpressionType::COMPARE_EQUAL, std::move(equal_three_child_exprs));

  auto one = std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(1));
  std::vector<std::unique_ptr<parser::AbstractExpression>> equal_child_exprs;
  equal_child_exprs.emplace_back(col_a.Copy());
  equal_child_exprs.emplace_back(std::move(one));
  parser::ComparisonExpression equals_one(parser::ExpressionType::COMPARE_EQUAL, std::move(equal_child_exprs));

  std::vector<std::unique_ptr<parser::AbstractExpression>> or_child_exprs;
  or_child_exprs.emplace_back(equals_three.Copy());
  or_child_exprs.emplace_back(equals_one.Copy());
  parser::ConjunctionExpression or_op(parser::ExpressionType::CONJUNCTION_OR, std::move(or_child_exprs));

  common::ManagedPointer<parser::AbstractExpression> or_expr(&or_op);
  AnnotatedExpression annotated_or(or_expr, {});

  Operator logical_get = LogicalGet::Make(test_db_oid_, table_oid_1_, {annotated_or}, table_name_1_, false)
                             .RegisterWithTxnContext(test_txn_);
  GroupExpression *gexpr = new GroupExpression(logical_get, {}, test_txn_);
  gexpr->SetGroupID(group_id_t(1));
  context_.GetMemo().InsertExpression(gexpr, false);

  ExprSet required_cols;
  required_cols.emplace(&col_a);

  stats_calculator_.CalculateStats(gexpr, &context_);

  auto *root_group = context_.GetMemo().GetGroupByID(gexpr->GetGroupID());
  // In reality it's two but that calculations aren't accurate enough to capture this
  EXPECT_EQ(root_group->GetNumRows(), 1);
  EXPECT_TRUE(root_group->HasNumRows());
}

// NOLINTNEXTLINE
TEST_F(StatsCalculatorTests, TestLogicalLimit) {
  RunQuery("INSERT INTO " + table_name_1_ + " VALUES(1), (NULL), (3);");
  RunQuery("ANALYZE " + table_name_1_ + ";");
  txn_manager_->Commit(test_txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
  test_txn_ = txn_manager_->BeginTransaction();

  // Constructing Logical Limit with logical get with no predicate from "empty_nullable_table"
  Operator logical_get =
      LogicalGet::Make(test_db_oid_, table_oid_1_, {}, table_name_1_, false).RegisterWithTxnContext(test_txn_);
  GroupExpression *get_gexpr = new GroupExpression(logical_get, {}, test_txn_);
  get_gexpr->SetGroupID(group_id_t(0));
  context_.GetMemo().InsertExpression(get_gexpr, true);

  Operator logical_limit = LogicalLimit::Make(0, 2, {}, {}).RegisterWithTxnContext(test_txn_);
  GroupExpression *limit_gexpr = new GroupExpression(logical_limit, {group_id_t(0)}, test_txn_);
  limit_gexpr->SetGroupID(group_id_t(1));
  context_.GetMemo().InsertExpression(limit_gexpr, false);

  parser::ColumnValueExpression col_a(table_name_1_, table_1_col_1_name_, test_db_oid_, table_oid_1_, table_1_col_oid_,
                                      type::TypeId::INTEGER);
  ExprSet required_cols;
  required_cols.emplace(&col_a);

  stats_calculator_.CalculateStats(get_gexpr, &context_);
  stats_calculator_.CalculateStats(limit_gexpr, &context_);

  auto *root_group = context_.GetMemo().GetGroupByID(limit_gexpr->GetGroupID());
  EXPECT_EQ(root_group->GetNumRows(), 2);
  EXPECT_TRUE(root_group->HasNumRows());
  auto *get_group = context_.GetMemo().GetGroupByID(get_gexpr->GetGroupID());
  EXPECT_EQ(get_group->GetNumRows(), 3);
  EXPECT_TRUE(get_group->HasNumRows());
}

// NOLINTNEXTLINE
TEST_F(StatsCalculatorTests, TestLogicalSemiJoin) {
  RunQuery("INSERT INTO " + table_name_1_ + " VALUES(1), (2);");
  RunQuery("ANALYZE " + table_name_1_ + ";");

  RunQuery("INSERT INTO " + table_name_2_ + " VALUES(1, TRUE), (1, FALSE), (5, TRUE), (666, FALSE);");
  RunQuery("ANALYZE " + table_name_2_ + ";");
  txn_manager_->Commit(test_txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
  test_txn_ = txn_manager_->BeginTransaction();

  // Constructing Logical Semi Join with join predicates "JOIN empty_nullable_table AND empty_table2
  // ON empty_nullable_table.colA = empty_table2.colA"
  Operator logical_get1 =
      LogicalGet::Make(test_db_oid_, table_oid_1_, {}, table_name_1_, false).RegisterWithTxnContext(test_txn_);
  GroupExpression *get_gexpr1 = new GroupExpression(logical_get1, {}, test_txn_);
  get_gexpr1->SetGroupID(group_id_t(0));
  context_.GetMemo().InsertExpression(get_gexpr1, true);

  Operator logical_get2 =
      LogicalGet::Make(test_db_oid_, table_oid_2_, {}, table_name_2_, false).RegisterWithTxnContext(test_txn_);
  GroupExpression *get_gexpr2 = new GroupExpression(logical_get2, {}, test_txn_);
  get_gexpr2->SetGroupID(group_id_t(1));
  context_.GetMemo().InsertExpression(get_gexpr2, true);

  parser::ColumnValueExpression col_a1(table_name_1_, table_1_col_1_name_, test_db_oid_, table_oid_1_, table_1_col_oid_,
                                       type::TypeId::INTEGER);
  parser::ColumnValueExpression col_a2(table_name_2_, table_2_col_1_name_, test_db_oid_, table_oid_2_,
                                       table_2_col_1_oid_, type::TypeId::INTEGER);
  parser::ColumnValueExpression col_b(table_name_2_, table_2_col_2_name_, test_db_oid_, table_oid_2_,
                                      table_2_col_2_oid_, type::TypeId::INTEGER);

  std::vector<std::unique_ptr<parser::AbstractExpression>> equal_child_exprs;
  equal_child_exprs.emplace_back(col_a1.Copy());
  equal_child_exprs.emplace_back(col_a2.Copy());
  parser::ComparisonExpression equals(parser::ExpressionType::COMPARE_EQUAL, std::move(equal_child_exprs));
  common::ManagedPointer<parser::AbstractExpression> equal_expr(&equals);
  AnnotatedExpression annotated_equals(equal_expr, {});

  Operator logical_semi_join = LogicalSemiJoin::Make({annotated_equals}).RegisterWithTxnContext(test_txn_);
  GroupExpression *join_gexpr = new GroupExpression(logical_semi_join, {group_id_t(0), group_id_t(1)}, test_txn_);
  join_gexpr->SetGroupID(group_id_t(2));
  context_.GetMemo().InsertExpression(join_gexpr, false);

  ExprSet get1_required_cols;
  ExprSet get2_required_cols;
  ExprSet join_required_cols;
  get1_required_cols.emplace(&col_a1);
  get2_required_cols.emplace(&col_a2);
  get2_required_cols.emplace(&col_b);
  join_required_cols.emplace(&col_a1);
  join_required_cols.emplace(&col_a2);
  join_required_cols.emplace(&col_b);

  stats_calculator_.CalculateStats(get_gexpr1, &context_);
  stats_calculator_.CalculateStats(get_gexpr2, &context_);
  stats_calculator_.CalculateStats(join_gexpr, &context_);

  auto *root_group = context_.GetMemo().GetGroupByID(join_gexpr->GetGroupID());
  EXPECT_EQ(root_group->GetNumRows(), 2);
  auto *get1_group = context_.GetMemo().GetGroupByID(get_gexpr1->GetGroupID());
  EXPECT_EQ(get1_group->GetNumRows(), 2);
  EXPECT_TRUE(get1_group->HasNumRows());
  auto *get2_group = context_.GetMemo().GetGroupByID(get_gexpr2->GetGroupID());
  EXPECT_EQ(get2_group->GetNumRows(), 4);
  EXPECT_TRUE(get2_group->HasNumRows());
}

// NOLINTNEXTLINE
TEST_F(StatsCalculatorTests, TestLogicalInnerJoin) {
  RunQuery("INSERT INTO " + table_name_1_ + " VALUES(1), (2);");
  RunQuery("ANALYZE " + table_name_1_ + ";");

  RunQuery("INSERT INTO " + table_name_2_ + " VALUES(1, TRUE), (1, FALSE), (5, TRUE), (666, FALSE);");
  RunQuery("ANALYZE " + table_name_2_ + ";");
  txn_manager_->Commit(test_txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
  test_txn_ = txn_manager_->BeginTransaction();

  // Constructing Logical Inner Join with join predicates "JOIN empty_nullable_table AND empty_table2
  //  ON empty_nullable_table.colA = empty_table2.colA"
  Operator logical_get1 =
      LogicalGet::Make(test_db_oid_, table_oid_1_, {}, table_name_1_, false).RegisterWithTxnContext(test_txn_);
  GroupExpression *get_gexpr1 = new GroupExpression(logical_get1, {}, test_txn_);
  get_gexpr1->SetGroupID(group_id_t(0));
  context_.GetMemo().InsertExpression(get_gexpr1, true);

  Operator logical_get2 =
      LogicalGet::Make(test_db_oid_, table_oid_2_, {}, table_name_2_, false).RegisterWithTxnContext(test_txn_);
  GroupExpression *get_gexpr2 = new GroupExpression(logical_get2, {}, test_txn_);
  get_gexpr2->SetGroupID(group_id_t(1));
  context_.GetMemo().InsertExpression(get_gexpr2, true);

  parser::ColumnValueExpression col_a1(table_name_1_, table_1_col_1_name_, test_db_oid_, table_oid_1_, table_1_col_oid_,
                                       type::TypeId::INTEGER);
  parser::ColumnValueExpression col_a2(table_name_2_, table_2_col_1_name_, test_db_oid_, table_oid_2_,
                                       table_2_col_1_oid_, type::TypeId::INTEGER);
  parser::ColumnValueExpression col_b(table_name_2_, table_2_col_2_name_, test_db_oid_, table_oid_2_,
                                      table_2_col_2_oid_, type::TypeId::INTEGER);

  std::vector<std::unique_ptr<parser::AbstractExpression>> equal_child_exprs;
  equal_child_exprs.emplace_back(col_a1.Copy());
  equal_child_exprs.emplace_back(col_a2.Copy());
  parser::ComparisonExpression equals(parser::ExpressionType::COMPARE_EQUAL, std::move(equal_child_exprs));
  common::ManagedPointer<parser::AbstractExpression> equal_expr(&equals);
  AnnotatedExpression annotated_equals(equal_expr, {});

  Operator logical_inner_join = LogicalInnerJoin::Make({annotated_equals}).RegisterWithTxnContext(test_txn_);
  GroupExpression *join_gexpr = new GroupExpression(logical_inner_join, {group_id_t(0), group_id_t(1)}, test_txn_);
  join_gexpr->SetGroupID(group_id_t(2));
  context_.GetMemo().InsertExpression(join_gexpr, false);

  ExprSet get1_required_cols;
  ExprSet get2_required_cols;
  ExprSet join_required_cols;
  get1_required_cols.emplace(&col_a1);
  get2_required_cols.emplace(&col_a2);
  get2_required_cols.emplace(&col_b);
  join_required_cols.emplace(&col_a1);
  join_required_cols.emplace(&col_a2);
  join_required_cols.emplace(&col_b);

  stats_calculator_.CalculateStats(get_gexpr1, &context_);
  stats_calculator_.CalculateStats(get_gexpr2, &context_);
  stats_calculator_.CalculateStats(join_gexpr, &context_);

  auto *root_group = context_.GetMemo().GetGroupByID(join_gexpr->GetGroupID());
  EXPECT_EQ(root_group->GetNumRows(), 2);
  EXPECT_TRUE(root_group->HasNumRows());
  auto *get1_group = context_.GetMemo().GetGroupByID(get_gexpr1->GetGroupID());
  EXPECT_EQ(get1_group->GetNumRows(), 2);
  EXPECT_TRUE(get1_group->HasNumRows());
  auto *get2_group = context_.GetMemo().GetGroupByID(get_gexpr2->GetGroupID());
  EXPECT_EQ(get2_group->GetNumRows(), 4);
  EXPECT_TRUE(get2_group->HasNumRows());
}

}  // namespace noisepage::optimizer
