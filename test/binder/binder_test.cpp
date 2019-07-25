#include <memory>
#include "util/transaction_benchmark_util.h"
#include "transaction/transaction_manager.h"
#include "util/test_harness.h"
#include "binder/bind_node_visitor.h"
#include "catalog/catalog.h"
#include "traffic_cop/statement.h"
#include "storage/garbage_collector.h"
#include "parser/expression/subquery_expression.h"
#include "parser/expression/column_value_expression.h"
#include "parser/postgresparser.h"
#include "traffic_cop/traffic_cop.h"

using std::make_shared;
using std::make_tuple;

using std::unique_ptr;
using std::vector;

namespace terrier {

// TODO (Ling): write meaningful setup
class BinderCorrectnessTest : public TerrierTest {
 private:
  storage::RecordBufferSegmentPool buffer_pool_{1000000, 1000000};
  storage::BlockStore block_store_{1000, 1000};
  catalog::Catalog *catalog_;
  std::string default_ns_name_ = "test_ns";
  catalog::namespace_oid_t ns_oid_;
  storage::GarbageCollector *gc_;
 protected:
  std::string default_database_name_ = "test_db";
  catalog::db_oid_t db_oid_;
  catalog::table_oid_t table_a_oid_;
  catalog::table_oid_t table_b_oid_;
  parser::PostgresParser parser_;
  transaction::TransactionManager txn_manager_{&buffer_pool_, true, LOGGING_DISABLED};
  transaction::TransactionContext *txn_;
  catalog::CatalogAccessor *accessor_;
  binder::BindNodeVisitor *binder_;

  void flush() {
    // Run the GC to flush it down to a clean system
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
  }

  void SetUpTables() {
    // This function should be run before the starting of first testcase

    gc_ = new storage::GarbageCollector(&txn_manager_);
    txn_ = txn_manager_.BeginTransaction();
    // new catalog requires txn_manage and block_store as parameters
    catalog_ = new catalog::Catalog(&txn_manager_, &block_store_);
    txn_manager_.Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
    flush();

    // create database
    txn_ = txn_manager_.BeginTransaction();
    LOG_INFO("Creating database %s", default_database_name_.c_str());
    db_oid_ = catalog_->CreateDatabase(txn_, default_database_name_, true);
    // commit the transactions
    txn_manager_.Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
    LOG_INFO("database %s created!", default_database_name_.c_str());
    flush();

    // Create namespace
    txn_ = txn_manager_.BeginTransaction();
    // get the catalog accessor
    accessor_ = catalog_->GetAccessor(txn_, db_oid_);
    // create a namespace
    ns_oid_ = accessor_->CreateNamespace(default_ns_name_);
    accessor_->SetSearchPath(std::vector<catalog::namespace_oid_t>{ns_oid_});
    txn_manager_.Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
    flush();

    // get default values of the columns
    auto int_default = parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER));
    auto varchar_default = parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::VARCHAR));

    // create table A
    txn_ = txn_manager_.BeginTransaction();
    accessor_ = catalog_->GetAccessor(txn_, db_oid_);
    // Create the column definition (no OIDs) for CREATE TABLE A(A1 int, a2 varchar)
    std::vector<catalog::Schema::Column> cols_a;
    cols_a.emplace_back("A1", type::TypeId::INTEGER, true, int_default);
    cols_a.emplace_back("a2", type::TypeId::VARCHAR, 20, true, varchar_default);
    auto schema_a = catalog::Schema(cols_a);

    table_a_oid_ = accessor_->CreateTable(ns_oid_, "A", schema_a);
    txn_manager_.Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
    flush();

    // create Table B
    txn_ = txn_manager_.BeginTransaction();
    accessor_ = catalog_->GetAccessor(txn_, db_oid_);
    // Create the column definition (no OIDs) for CREATE TABLE b(b1 int, B2 varchar)
    std::vector<catalog::Schema::Column> cols_b;
    cols_b.emplace_back("b1", type::TypeId::INTEGER, true, int_default);
    cols_b.emplace_back("B2", type::TypeId::VARCHAR, 20, true, varchar_default);

    auto schema_b = catalog::Schema(cols_b);
    table_b_oid_ = accessor_->CreateTable(ns_oid_, "b", schema_b);
    txn_manager_.Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
    flush();
  }

  void TearDownTables() {
    // This function should be run after the end of last testcase

    // Delete the test database
    txn_ = txn_manager_.BeginTransaction();
    accessor_->DropDatabase(db_oid_);
    txn_manager_.Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
    flush();
    delete gc_;
    delete catalog_;
  }

  virtual void SetUp() override {
    TerrierTest::SetUp();
    SetUpTables();
    // prepare for testing
    txn_ = txn_manager_.BeginTransaction();
    accessor_ = catalog_->GetAccessor(txn_, db_oid_);
    binder_ = new binder::BindNodeVisitor(accessor_, default_database_name_);
  }

  virtual void TearDown() override {
    TerrierTest::TearDown();
    txn_manager_.Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
    delete binder_;
    binder_ = nullptr;
    TearDownTables();
  }
};

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, SelectStatementComplexTest) {
  // Test regular table name
  LOG_INFO("Parsing sql query");

  std::string selectSQL = "SELECT A.a1, B.b2 FROM A INNER JOIN b ON a.a1 = b.b1 WHERE a1 < 100 "
                          "GROUP BY A.a1, B.b2 HAVING a1 > 50 ORDER BY a1";

  auto parse_tree = parser_.BuildParseTree(selectSQL);
  auto selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree[0].get());
  binder_->BindNameToNode(selectStmt);

  // Check select_list
  LOG_INFO("Checking select list");
  auto col_expr = dynamic_cast<const parser::ColumnValueExpression *>(selectStmt->GetSelectColumns()[0].get());
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_); //A.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_); //A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1)); // A.a1; columns are indexed from 1

  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
  col_expr = (parser::ColumnValueExpression *) selectStmt->GetSelectColumns()[1].get();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_); //B.b2
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_); //B.b2
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(2)); // B.b2; columns are indexed from 1

  EXPECT_EQ(type::TypeId::VARCHAR, col_expr->GetReturnValueType());

  // Check join condition
  LOG_INFO("Checking join condition");
  col_expr =
      (parser::ColumnValueExpression *) selectStmt->GetSelectTable()->GetJoin()->GetJoinCondition()->GetChild(0).get();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_); //A.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_); //A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1)); // A.a1; columns are indexed from 1

  col_expr =
      (parser::ColumnValueExpression *) selectStmt->GetSelectTable()->GetJoin()->GetJoinCondition()->GetChild(1).get();

//  EXPECT_EQ(col_expr->GetBoundOid(), make_tuple(db_oid, table_b_oid_, 0));  // b.b1
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_); //B.b1
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_); //B.b1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1)); // B.b1; columns are indexed from 1

  // Check Where clause
  LOG_INFO("Checking where clause");
  col_expr = (parser::ColumnValueExpression *) selectStmt->GetSelectCondition()->GetChild(0).get();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_); //A.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_); //A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1)); // A.a1; columns are indexed from 1

  // Check Group By and Having
  LOG_INFO("Checking group by");
  col_expr = (parser::ColumnValueExpression *) selectStmt->GetSelectGroupBy()->GetColumns()[0].get();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_); //A.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_); //A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1)); // A.a1; columns are indexed from 1

  col_expr = dynamic_cast<const parser::ColumnValueExpression *>(selectStmt->GetSelectGroupBy()->GetColumns()[1].get());
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_); //B.b2
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_); //B.b2
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(2)); // B.b2; columns are indexed from 1

  col_expr = (parser::ColumnValueExpression *) selectStmt->GetSelectGroupBy()->GetHaving()->GetChild(0).get();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_); //A.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_); //A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1)); // A.a1; columns are indexed from 1

  // Check Order By
  LOG_INFO("Checking order by");
  col_expr = (parser::ColumnValueExpression *) selectStmt->GetSelectOrderBy()->GetOrderByExpressions()[0].get();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_); //A.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_); //A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1)); // A.a1; columns are indexed from 1
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, SelectStatementDupAliasTest) {
  // Check alias ambiguous
  LOG_INFO("Checking duplicate alias and table name.");

  std::string selectSQL = "SELECT * FROM A, B as A";
  auto parse_tree = parser_.BuildParseTree(selectSQL);
  auto selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree[0].get());
#ifndef NDEBUG
  EXPECT_DEATH(binder_->BindNameToNode(selectStmt), "Duplicate alias a");
#endif
//  try {
//    binder_->BindNameToNode(selectStmt);
//    EXPECT_TRUE(false);
//  } catch (Exception &e) {
//    LOG_INFO("Correct! Exception(%s) catched", e.what());
//  }
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, SelectStatementDiffTableSameSchemaTest) {
  // Test select from different table instances from the same physical schema
  std::string selectSQL = "SELECT * FROM A, A as AA where A.a1 = AA.a2";
  auto parse_tree = parser_.BuildParseTree(selectSQL);
  auto selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree[0].get());
  binder_->BindNameToNode(selectStmt);
  LOG_INFO("Checking where clause");
  auto col_expr =
      dynamic_cast<const parser::ColumnValueExpression *>(selectStmt->GetSelectCondition()->GetChild(0).get());
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_); //A.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_); //A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1)); // A.a1; columns are indexed from 1

  col_expr = dynamic_cast<const parser::ColumnValueExpression *>(selectStmt->GetSelectCondition()->GetChild(1).get());
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_); //AA.a2
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_); //AA.a2
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(2)); // AA.a2; columns are indexed from 1
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, SelectStatementSelectListAliasTest) {
  // Test alias and select_list
  LOG_INFO("Checking select_list and table alias binding");

  std::string selectSQL = "SELECT AA.a1, b2 FROM A as AA, B WHERE AA.a1 = B.b1";
  auto parse_tree = parser_.BuildParseTree(selectSQL);
  auto selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree[0].get());
  binder_->BindNameToNode(selectStmt);
  auto col_expr = dynamic_cast<parser::ColumnValueExpression *>(selectStmt->GetSelectColumns()[0].get());
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_); //AA.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_); //AA.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1)); // AA.a1; columns are indexed from 1

  col_expr = dynamic_cast<parser::ColumnValueExpression *>(selectStmt->GetSelectColumns()[1].get());
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_); //b2
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_); //b2
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(2)); // b2; columns are indexed from 1
}

// TODO: add test for Update Statement. Currently UpdateStatement uses char*
// instead of ColumnValueExpression to represent column. We can only add this
// test after UpdateStatement is changed

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, DeleteStatementWhereTest) {
  std::string deleteSQL = "DELETE FROM b WHERE 1 = b1 AND b2 = 'str'";
  auto parse_tree = parser_.BuildParseTree(deleteSQL);
  auto deleteStmt = dynamic_cast<parser::DeleteStatement *>(parse_tree[0].get());
  binder_->BindNameToNode(deleteStmt);

  LOG_INFO("Checking first condition in where clause");
  auto col_expr =
      dynamic_cast<const parser::ColumnValueExpression *>(deleteStmt->GetDeleteCondition()->GetChild(0)->GetChild(1).get());
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_); //b1
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_); //b1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1)); // b1; columns are indexed from 1

  LOG_INFO("Checking second condition in where clause");
  col_expr =
      dynamic_cast<const parser::ColumnValueExpression *>(deleteStmt->GetDeleteCondition()->GetChild(1)->GetChild(0).get());
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_); //b2
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_); //b2
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(2)); // b2; columns are indexed from 1
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, BindDepthTest) {
  // Test regular table name
  LOG_INFO("Parsing sql query");

  std::string selectSQL = "SELECT A.a1 FROM A WHERE A.a1 IN (SELECT b1 FROM B WHERE b1 = 2 AND "
                          "b2 > (SELECT a1 FROM A WHERE a2 > 0)) AND EXISTS (SELECT b1 FROM B WHERE B.b1 = A.a1)";

  auto parse_tree = parser_.BuildParseTree(selectSQL);
  auto selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree[0].get());
  binder_->BindNameToNode(selectStmt);

  // Check select depth
  EXPECT_EQ(0, selectStmt->GetDepth());

  // Check select_list
  LOG_INFO("Checking select list");
  auto tv_expr = selectStmt->GetSelectColumns()[0].get();
  EXPECT_EQ(0, tv_expr->GetDepth());  // A.a1

  // Check Where clause
  LOG_INFO("Checking where clause");
  EXPECT_EQ(0, selectStmt->GetSelectCondition()->GetDepth()); // XXX AND YYY

  // A.a1 IN (SELECT b1 FROM B WHERE b1 = 2 AND b2 > (SELECT a1 FROM A WHERE a2 > 0))
  auto in_expr = selectStmt->GetSelectCondition()->GetChild(0); // A compare_in expression
  auto in_tv_expr = in_expr->GetChild(0); // A.a1
  // SELECT b1 FROM B WHERE b1 = 2 AND b2 > (SELECT a1 FROM A WHERE a2 > 0)
  auto in_sub_expr = in_expr->GetChild(1);
  auto in_sub_expr_select = dynamic_cast<parser::SubqueryExpression *>(in_sub_expr.get())->GetSubselect();
  auto in_sub_expr_select_ele = in_sub_expr_select->GetSelectColumns()[0].get(); // b1
  // WHERE b1 = 2 AND b2 > (SELECT a1 FROM A WHERE a2 > 0)
  auto in_sub_expr_select_where = in_sub_expr_select->GetSelectCondition().get();
  // b1 = 2
  auto in_sub_expr_select_where_left = in_sub_expr_select_where->GetChild(0);
  // b2 > (SELECT a1 FROM A WHERE a2 > 0)
  auto in_sub_expr_select_where_right = in_sub_expr_select_where->GetChild(1);
  auto in_sub_expr_select_where_right_tv = in_sub_expr_select_where_right->GetChild(0); // b2
  // SELECT a1 FROM A WHERE a2 > 0
  auto in_sub_expr_select_where_right_sub = in_sub_expr_select_where_right->GetChild(1);
  auto in_sub_expr_select_where_right_sub_select =
      dynamic_cast<parser::SubqueryExpression *>(in_sub_expr_select_where_right_sub.get())->GetSubselect();
  // WHERE a2 > 0
  auto in_sub_expr_select_where_right_sub_select_where =
      in_sub_expr_select_where_right_sub_select->GetSelectCondition().get();
  // a1
  auto in_sub_expr_select_where_right_sub_select_ele =
      in_sub_expr_select_where_right_sub_select->GetSelectColumns()[0].get();

  // EXISTS (SELECT b1 FROM B WHERE B.b1 = A.a1)
  auto exists_expr = selectStmt->GetSelectCondition()->GetChild(1); // An operator_exists expression
  auto exists_sub_expr = exists_expr->GetChild(0);
  // a select statement: SELECT b1 FROM B WHERE B.b1 = A.a1
  auto exists_sub_expr_select = dynamic_cast<parser::SubqueryExpression *>(exists_sub_expr.get())->GetSubselect();
  // WHERE B.b1 = A.a1
  auto exists_sub_expr_select_where = exists_sub_expr_select->GetSelectCondition().get();
  auto exists_sub_expr_select_ele = exists_sub_expr_select->GetSelectColumns()[0].get(); // b1

  EXPECT_EQ(0, in_expr->GetDepth());
  EXPECT_EQ(0, exists_expr->GetDepth());
  EXPECT_EQ(0, exists_sub_expr->GetDepth());
  EXPECT_EQ(1, exists_sub_expr_select->GetDepth());
  EXPECT_EQ(0, exists_sub_expr_select_where->GetDepth());
  EXPECT_EQ(1, exists_sub_expr_select_ele->GetDepth());
  EXPECT_EQ(0, in_tv_expr->GetDepth());
  EXPECT_EQ(1, in_sub_expr->GetDepth());
  EXPECT_EQ(1, in_sub_expr_select->GetDepth());
  EXPECT_EQ(1, in_sub_expr_select_where->GetDepth());
  EXPECT_EQ(1, in_sub_expr_select_ele->GetDepth());
  EXPECT_EQ(1, in_sub_expr_select_where_left->GetDepth());
  EXPECT_EQ(1, in_sub_expr_select_where_right->GetDepth());
  EXPECT_EQ(1, in_sub_expr_select_where_right_tv->GetDepth());
  EXPECT_EQ(2, in_sub_expr_select_where_right_sub->GetDepth());
  EXPECT_EQ(2, in_sub_expr_select_where_right_sub_select->GetDepth());
  EXPECT_EQ(2, in_sub_expr_select_where_right_sub_select_where->GetDepth());
  EXPECT_EQ(2, in_sub_expr_select_where_right_sub_select_ele->GetDepth());
}

// NOLINTNEXTLINE
//TEST_F(BinderCorrectnessTest, FunctionExpressionTest) {
//  std::string function_sql = "SELECT substr('test123', a, 3)";
//  auto parse_tree = parser_.BuildParseTree(function_sql);
//  auto stmt = parse_tree[0].get();
//  // TODO (ling): figure out specific exception that would be thrown
//  EXPECT_THROW(binder_->BindNameToNode(stmt), terrier::Exception);
//
//  function_sql = "SELECT substr('test123', 2, 3)";
//  auto parse_tree2 = parser_.BuildParseTree(function_sql);
//  stmt = parse_tree2[0].get();
//  binder_->BindNameToNode(stmt);
//  auto funct_expr = dynamic_cast<parser::FunctionExpression *>(dynamic_cast<parser::SelectStatement *>(stmt)->select_list[0].get());
//  EXPECT_TRUE(funct_expr->Evaluate(nullptr, nullptr, nullptr).CompareEquals(type::ValueFactory::GetVarcharValue("est")) == CmpBool::CmpTrue);
//}
}