#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "binder/bind_node_visitor.h"
#include "catalog/catalog.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/operator_expression.h"
#include "parser/expression/subquery_expression.h"
#include "parser/postgresparser.h"
#include "storage/garbage_collector.h"
#include "traffic_cop/statement.h"
#include "traffic_cop/traffic_cop.h"
#include "transaction/transaction_manager.h"
#include "util/test_harness.h"
#include "util/transaction_benchmark_util.h"

using std::make_shared;
using std::make_tuple;

using std::unique_ptr;
using std::vector;

namespace terrier {

// TODO(Ling): write meaningful setup
class BinderCorrectnessTest : public TerrierTest {
 private:
  storage::RecordBufferSegmentPool buffer_pool_{1000000, 1000000};
  storage::BlockStore block_store_{1000, 1000};
  catalog::Catalog *catalog_;
  storage::GarbageCollector *gc_;

 protected:
  std::string default_database_name_ = "test_db";
  catalog::db_oid_t db_oid_;
  catalog::table_oid_t table_a_oid_;
  catalog::table_oid_t table_b_oid_;
  parser::PostgresParser parser_;
  transaction::TransactionManager *txn_manager_;
  transaction::TransactionContext *txn_;
  std::unique_ptr<catalog::CatalogAccessor> accessor_;
  binder::BindNodeVisitor *binder_;

  void SetUpTables() {
    txn_manager_ = new transaction::TransactionManager(&buffer_pool_, true, LOGGING_DISABLED);
    gc_ = new storage::GarbageCollector(txn_manager_, nullptr);
    // new catalog requires txn_manage and block_store as parameters
    catalog_ = new catalog::Catalog(txn_manager_, &block_store_);

    // create database
    txn_ = txn_manager_->BeginTransaction();
    LOG_INFO("Creating database %s", default_database_name_.c_str());
    db_oid_ = catalog_->CreateDatabase(txn_, default_database_name_, true);
    // commit the transactions
    txn_manager_->Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
    LOG_INFO("database %s created!", default_database_name_.c_str());

    // get default values of the columns
    auto int_default = parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER));
    auto varchar_default = parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::VARCHAR));

    // TODO(Ling): use mixed case in table name and schema name
    //  for testcases to see if the binder does not differentiate between upper and lower cases
    // create table A
    txn_ = txn_manager_->BeginTransaction();
    accessor_ = catalog_->GetAccessor(txn_, db_oid_);
    // Create the column definition (no OIDs) for CREATE TABLE A(A1 int, a2 varchar)
    std::vector<catalog::Schema::Column> cols_a;
    cols_a.emplace_back("a1", type::TypeId::INTEGER, true, int_default);
    cols_a.emplace_back("a2", type::TypeId::VARCHAR, 20, true, varchar_default);
    auto schema_a = catalog::Schema(cols_a);

    table_a_oid_ = accessor_->CreateTable(accessor_->GetDefaultNamespace(), "a", schema_a);
    auto table_a = new storage::SqlTable(&block_store_, schema_a);
    EXPECT_TRUE(accessor_->SetTablePointer(table_a_oid_, table_a));

    txn_manager_->Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
    accessor_.reset(nullptr);

    // create Table B
    txn_ = txn_manager_->BeginTransaction();
    accessor_ = catalog_->GetAccessor(txn_, db_oid_);

    // Create the column definition (no OIDs) for CREATE TABLE b(b1 int, B2 varchar)
    std::vector<catalog::Schema::Column> cols_b;
    cols_b.emplace_back("b1", type::TypeId::INTEGER, true, int_default);
    cols_b.emplace_back("b2", type::TypeId::VARCHAR, 20, true, varchar_default);

    auto schema_b = catalog::Schema(cols_b);
    table_b_oid_ = accessor_->CreateTable(accessor_->GetDefaultNamespace(), "b", schema_b);
    auto table_b = new storage::SqlTable(&block_store_, schema_b);
    EXPECT_TRUE(accessor_->SetTablePointer(table_b_oid_, table_b));
    txn_manager_->Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
    accessor_.reset(nullptr);
  }

  void TearDownTables() {
    catalog_->TearDown();
    // Run the GC to flush it down to a clean system
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
    delete catalog_;
    delete gc_;
    delete txn_manager_;
  }

  void SetUp() override {
    TerrierTest::SetUp();
    SetUpTables();
    // prepare for testing
    txn_ = txn_manager_->BeginTransaction();
    accessor_ = catalog_->GetAccessor(txn_, db_oid_);
    binder_ = new binder::BindNodeVisitor(std::move(accessor_), default_database_name_);
  }

  void TearDown() override {
    txn_manager_->Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
    accessor_.reset(nullptr);
    delete binder_;
    TearDownTables();
    TerrierTest::TearDown();
  }
};

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, SelectStatementComplexTest) {
  // Test regular table name
  LOG_INFO("Parsing sql query");
  std::string selectSQL =
      "SELECT A.A1, B.B2 FROM A INNER JOIN b ON a.a1 = b.b1 WHERE a1 < 100 "
      "GROUP BY A.a1, B.b2 HAVING a1 > 50 ORDER BY a1";

  auto parse_tree = parser_.BuildParseTree(selectSQL);
  auto selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree[0].get());
  binder_->BindNameToNode(selectStmt);
  EXPECT_EQ(0, selectStmt->GetDepth());

  // Check select_list
  LOG_INFO("Checking select list");
  auto col_expr = dynamic_cast<const parser::ColumnValueExpression *>(selectStmt->GetSelectColumns()[0].get());
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // A.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_);            // A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // A.a1; columns are indexed from 1
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
  EXPECT_EQ(0, col_expr->GetDepth());

  col_expr = dynamic_cast<const parser::ColumnValueExpression *>(selectStmt->GetSelectColumns()[1].get());
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // B.b2
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_);            // B.b2
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(2));  // B.b2; columns are indexed from 1
  EXPECT_EQ(type::TypeId::VARCHAR, col_expr->GetReturnValueType());
  EXPECT_EQ(0, col_expr->GetDepth());

  // Check join condition
  LOG_INFO("Checking join condition");
  col_expr = dynamic_cast<const parser::ColumnValueExpression *>(
      selectStmt->GetSelectTable()->GetJoin()->GetJoinCondition()->GetChild(0).get());
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // A.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_);            // A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // A.a1; columns are indexed from 1
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
  EXPECT_EQ(0, col_expr->GetDepth());

  col_expr = dynamic_cast<const parser::ColumnValueExpression *>(
      selectStmt->GetSelectTable()->GetJoin()->GetJoinCondition()->GetChild(1).get());

  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // B.b1
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_);            // B.b1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // B.b1; columns are indexed from 1
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
  EXPECT_EQ(0, col_expr->GetDepth());

  // Check Where clause
  LOG_INFO("Checking where clause");
  col_expr = dynamic_cast<const parser::ColumnValueExpression *>(selectStmt->GetSelectCondition()->GetChild(0).get());
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // A.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_);            // A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // A.a1; columns are indexed from 1
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
  EXPECT_EQ(0, col_expr->GetDepth());

  // Check Group By and Having
  LOG_INFO("Checking group by");
  col_expr = dynamic_cast<const parser::ColumnValueExpression *>(selectStmt->GetSelectGroupBy()->GetColumns()[0].get());
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // A.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_);            // A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // A.a1; columns are indexed from 1
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
  EXPECT_EQ(0, col_expr->GetDepth());

  col_expr = dynamic_cast<const parser::ColumnValueExpression *>(selectStmt->GetSelectGroupBy()->GetColumns()[1].get());
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // B.b2
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_);            // B.b2
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(2));  // B.b2; columns are indexed from 1
  EXPECT_EQ(type::TypeId::VARCHAR, col_expr->GetReturnValueType());
  EXPECT_EQ(0, col_expr->GetDepth());

  col_expr = dynamic_cast<const parser::ColumnValueExpression *>(
      selectStmt->GetSelectGroupBy()->GetHaving()->GetChild(0).get());
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // A.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_);            // A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // A.a1; columns are indexed from 1
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
  EXPECT_EQ(0, col_expr->GetDepth());

  // Check Order By
  LOG_INFO("Checking order by");
  col_expr = dynamic_cast<const parser::ColumnValueExpression *>(
      selectStmt->GetSelectOrderBy()->GetOrderByExpressions()[0].get());
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // A.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_);            // A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // A.a1; columns are indexed from 1
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
  EXPECT_EQ(0, col_expr->GetDepth());
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, SelectStatementStarTest) {
  // Check if star expression is correctly processed
  LOG_INFO("Checking STAR expression in select and subselect");

  std::string selectSQL = "SELECT * FROM A LEFT OUTER JOIN B ON A.A1 < B.B1";
  auto parse_tree = parser_.BuildParseTree(selectSQL);
  auto selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree[0].get());
  binder_->BindNameToNode(selectStmt);
  EXPECT_EQ(0, selectStmt->GetDepth());

  // Check select_list
  LOG_INFO("Checking select list expansion");

  auto columns = selectStmt->GetSelectColumns();
  EXPECT_EQ(columns.size(), 4);

  // check if all required columns exist regardless of order, are they come from traversing unordered maps

  bool a1_exists = false;
  bool a2_exists = false;
  bool b1_exists = false;
  bool b2_exists = false;
  for (auto &col_abs_expr : columns) {
    auto col_expr = dynamic_cast<const parser::ColumnValueExpression *>(col_abs_expr.get());
    EXPECT_EQ(0, col_expr->GetDepth());
    EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);

    if (col_expr->GetTableOid() == table_a_oid_) {
      EXPECT_EQ(col_expr->GetTableName(), "a");
      if (col_expr->GetColumnName() == "a1") {
        a1_exists = true;
        EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));
        EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
      }
      if (col_expr->GetColumnName() == "a2") {
        a2_exists = true;
        EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(2));
        EXPECT_EQ(type::TypeId::VARCHAR, col_expr->GetReturnValueType());
      }
    }
    if (col_expr->GetTableOid() == table_b_oid_) {
      EXPECT_EQ(col_expr->GetTableName(), "b");
      if (col_expr->GetColumnName() == "b1") {
        b1_exists = true;
        EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));
        EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
      }
      if (col_expr->GetColumnName() == "b2") {
        b2_exists = true;
        EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(2));
        EXPECT_EQ(type::TypeId::VARCHAR, col_expr->GetReturnValueType());
      }
    }
  }

  EXPECT_TRUE(a1_exists);
  EXPECT_TRUE(a2_exists);
  EXPECT_TRUE(b1_exists);
  EXPECT_TRUE(b2_exists);
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, SelectStatementStarNestedSelectTest) {
  // Check if star expression is correctly processed
  LOG_INFO("Checking STAR expression in nested select from.");

  std::string selectSQL =
      "SELECT * FROM A LEFT OUTER JOIN (SELECT * FROM B INNER JOIN A ON B1 = A1) AS C ON C.B2 = a.A1";
  auto parse_tree = parser_.BuildParseTree(selectSQL);
  auto selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree[0].get());
  binder_->BindNameToNode(selectStmt);
  EXPECT_EQ(0, selectStmt->GetDepth());

  // Check select_list
  LOG_INFO("Checking select list expansion");
  auto columns = selectStmt->GetSelectColumns();
  EXPECT_EQ(columns.size(), 6);

  // check if all required columns exist regardless of order, are they come from traversing unordered maps

  bool a1_exists = false;
  bool a2_exists = false;
  bool c_a1_exists = false;
  bool c_a2_exists = false;
  bool c_b1_exists = false;
  bool c_b2_exists = false;

  for (auto &col_abs_expr : columns) {
    auto col_expr = dynamic_cast<const parser::ColumnValueExpression *>(col_abs_expr.get());
    EXPECT_EQ(col_expr->GetDepth(), 0);  // not from derived subquery

    if (col_expr->GetDatabaseOid() == db_oid_ && col_expr->GetTableOid() == table_a_oid_) {
      EXPECT_EQ(col_expr->GetTableName(), "a");

      if (col_expr->GetColumnOid() == catalog::col_oid_t(1) &&
          type::TypeId::INTEGER == col_expr->GetReturnValueType()) {
        EXPECT_EQ(col_expr->GetColumnName(), "a1");

        a1_exists = true;
      }
      if (col_expr->GetColumnOid() == catalog::col_oid_t(2) &&
          type::TypeId::VARCHAR == col_expr->GetReturnValueType()) {
        a2_exists = true;
        EXPECT_EQ(col_expr->GetColumnName(), "a2");
      }
    }

    if (col_expr->GetDatabaseOid() == catalog::INVALID_DATABASE_OID &&
        col_expr->GetTableOid() == catalog::INVALID_TABLE_OID &&
        col_expr->GetColumnOid() == catalog::INVALID_COLUMN_OID) {
      EXPECT_EQ(col_expr->GetTableName(), "c");

      if (col_expr->GetColumnName() == "a1") {
        c_a1_exists = true;
        EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
      }
      if (col_expr->GetColumnName() == "a2") {
        c_a2_exists = true;
        EXPECT_EQ(type::TypeId::VARCHAR, col_expr->GetReturnValueType());
      }
      if (col_expr->GetColumnName() == "b1") {
        c_b1_exists = true;
        EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
      }
      if (col_expr->GetColumnName() == "b2") {
        c_b2_exists = true;
        EXPECT_EQ(type::TypeId::VARCHAR, col_expr->GetReturnValueType());
      }
    }
  }

  EXPECT_TRUE(a1_exists);
  EXPECT_TRUE(a2_exists);
  EXPECT_TRUE(c_a1_exists);
  EXPECT_TRUE(c_a2_exists);
  EXPECT_TRUE(c_b1_exists);
  EXPECT_TRUE(c_b2_exists);

  // Check join condition
  LOG_INFO("Checking join condition");
  auto col_expr = dynamic_cast<const parser::ColumnValueExpression *>(
      selectStmt->GetSelectTable()->GetJoin()->GetJoinCondition()->GetChild(0).get());
  EXPECT_EQ(col_expr->GetColumnName(), "b2");  // C.B2
  EXPECT_EQ(col_expr->GetTableName(), "c");
  EXPECT_EQ(col_expr->GetDatabaseOid(), catalog::INVALID_DATABASE_OID);
  EXPECT_EQ(col_expr->GetTableOid(), catalog::INVALID_TABLE_OID);
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::INVALID_COLUMN_OID);
  EXPECT_EQ(type::TypeId::VARCHAR, col_expr->GetReturnValueType());
  EXPECT_EQ(col_expr->GetDepth(), 0);  // not from derived subquery

  col_expr = dynamic_cast<const parser::ColumnValueExpression *>(
      selectStmt->GetSelectTable()->GetJoin()->GetJoinCondition()->GetChild(1).get());
  EXPECT_EQ(col_expr->GetColumnName(), "a1");                  // A.a1
  EXPECT_EQ(col_expr->GetTableName(), "a");                    // A.a1
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // A.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_);            // A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // A.a1; columns are indexed from 1
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
  EXPECT_EQ(col_expr->GetDepth(), 0);  // not from derived subquery

  // check right table
  LOG_INFO("Checking nested table of the join");
  auto right_tb = dynamic_cast<parser::SelectStatement *>(
      selectStmt->GetSelectTable()->GetJoin()->GetRightTable()->GetSelect().get());
  EXPECT_EQ(right_tb->GetDepth(), 1);  // 1 level subselect
  LOG_INFO("Checking nested column select list");

  columns = right_tb->GetSelectColumns();
  EXPECT_EQ(columns.size(), 4);

  // check if all required columns exist regardless of order, are they come from traversing unordered maps
  a1_exists = false;
  a2_exists = false;
  bool b1_exists = false;
  bool b2_exists = false;
  for (auto &col_abs_expr : columns) {
    col_expr = dynamic_cast<const parser::ColumnValueExpression *>(col_abs_expr.get());
    EXPECT_EQ(1, col_expr->GetDepth());
    EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);

    if (col_expr->GetTableOid() == table_a_oid_) {
      EXPECT_EQ(col_expr->GetTableName(), "a");
      if (col_expr->GetColumnName() == "a1") {
        a1_exists = true;
        EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));
        EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
      }
      if (col_expr->GetColumnName() == "a2") {
        a2_exists = true;
        EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(2));
        EXPECT_EQ(type::TypeId::VARCHAR, col_expr->GetReturnValueType());
      }
    }
    if (col_expr->GetTableOid() == table_b_oid_) {
      EXPECT_EQ(col_expr->GetTableName(), "b");
      if (col_expr->GetColumnName() == "b1") {
        b1_exists = true;
        EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));
        EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
      }
      if (col_expr->GetColumnName() == "b2") {
        b2_exists = true;
        EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(2));
        EXPECT_EQ(type::TypeId::VARCHAR, col_expr->GetReturnValueType());
      }
    }
  }
  EXPECT_TRUE(a1_exists);
  EXPECT_TRUE(a2_exists);
  EXPECT_TRUE(b1_exists);
  EXPECT_TRUE(b2_exists);

  LOG_INFO("Checking nested table's join condition");
  auto join = right_tb->GetSelectTable()->GetJoin()->GetJoinCondition();
  // TODO(Ling): the join condition expression still has depth -1;
  //  but the left and right column value expressions have correct depth;
  //  Left for the optimizer to decide if depth of the `ON A1 = B1` is necessary to be set to 1

  col_expr = dynamic_cast<const parser::ColumnValueExpression *>(join->GetChild(0).get());
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // B.b1
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_);            // B.b1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // B.b1; columns are indexed from 1
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
  EXPECT_EQ(1, col_expr->GetDepth());

  col_expr = dynamic_cast<const parser::ColumnValueExpression *>(join->GetChild(1).get());
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // A.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_);            // A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // A.a1; columns are indexed from 1
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
  EXPECT_EQ(1, col_expr->GetDepth());
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, SelectStatementNestedColumnTest) {
  // Check if nested select columns are correctly processed
  LOG_INFO("Checking nested select columns.");

  std::string selectSQL = "SELECT A1, (SELECT B2 FROM B where B2 IS NULL LIMIT 1) FROM A";
  auto parse_tree = parser_.BuildParseTree(selectSQL);
  auto selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree[0].get());
  binder_->BindNameToNode(selectStmt);
  EXPECT_EQ(0, selectStmt->GetDepth());

  // Check select_list
  LOG_INFO("Checking select list");
  auto col_expr = dynamic_cast<const parser::ColumnValueExpression *>(selectStmt->GetSelectColumns()[0].get());
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // A.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_);            // A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // A.a1; columns are indexed from 1
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
  EXPECT_EQ(0, col_expr->GetDepth());

  LOG_INFO("Checking nested select in select list");
  auto subquery = dynamic_cast<parser::SubqueryExpression *>(selectStmt->GetSelectColumns()[1].get());
  EXPECT_EQ(subquery->GetDepth(), 1);

  auto subselect = dynamic_cast<parser::SelectStatement *>(subquery->GetSubselect().get());
  EXPECT_EQ(subselect->GetDepth(), 1);

  LOG_INFO("Checking select list in nested select in select list");
  col_expr = dynamic_cast<const parser::ColumnValueExpression *>(subselect->GetSelectColumns()[0].get());
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // B.b2
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_);            // B.b2
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(2));  // B.b2; columns are indexed from 1
  EXPECT_EQ(type::TypeId::VARCHAR, col_expr->GetReturnValueType());
  EXPECT_EQ(1, col_expr->GetDepth());

  LOG_INFO("Checking where clause in nested select in select list");
  auto where = dynamic_cast<const parser::OperatorExpression *>(subselect->GetSelectCondition().get());
  EXPECT_EQ(where->GetDepth(), 1);

  col_expr = dynamic_cast<const parser::ColumnValueExpression *>(where->GetChild(0).get());
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // B.b2
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_);            // B.b2
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(2));  // B.b2; columns are indexed from 1
  EXPECT_EQ(type::TypeId::VARCHAR, col_expr->GetReturnValueType());
  EXPECT_EQ(1, col_expr->GetDepth());
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, SelectStatementDupAliasTest) {
  // Check alias ambiguous
  LOG_INFO("Checking duplicate alias and table name.");

  std::string selectSQL = "SELECT * FROM A, B as A";
  auto parse_tree = parser_.BuildParseTree(selectSQL);
  auto selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree[0].get());
  EXPECT_THROW(binder_->BindNameToNode(selectStmt), BinderException);
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
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // A.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_);            // A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // A.a1; columns are indexed from 1

  col_expr = dynamic_cast<const parser::ColumnValueExpression *>(selectStmt->GetSelectCondition()->GetChild(1).get());
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // AA.a2
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_);            // AA.a2
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(2));  // AA.a2; columns are indexed from 1
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
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // AA.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_);            // AA.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // AA.a1; columns are indexed from 1

  col_expr = dynamic_cast<parser::ColumnValueExpression *>(selectStmt->GetSelectColumns()[1].get());
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // b2
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_);            // b2
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(2));  // b2; columns are indexed from 1
}

// TODO(peloton): add test for Update Statement. Currently UpdateStatement uses char*
// instead of ColumnValueExpression to represent column. We can only add this
// test after UpdateStatement is changed

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, DeleteStatementWhereTest) {
  std::string deleteSQL = "DELETE FROM b WHERE 1 = b1 AND b2 = 'str'";
  auto parse_tree = parser_.BuildParseTree(deleteSQL);
  auto deleteStmt = dynamic_cast<parser::DeleteStatement *>(parse_tree[0].get());
  binder_->BindNameToNode(deleteStmt);

  LOG_INFO("Checking first condition in where clause");
  auto col_expr = dynamic_cast<const parser::ColumnValueExpression *>(
      deleteStmt->GetDeleteCondition()->GetChild(0)->GetChild(1).get());
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // b1
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_);            // b1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // b1; columns are indexed from 1

  LOG_INFO("Checking second condition in where clause");
  col_expr = dynamic_cast<const parser::ColumnValueExpression *>(
      deleteStmt->GetDeleteCondition()->GetChild(1)->GetChild(0).get());
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // b2
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_);            // b2
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(2));  // b2; columns are indexed from 1
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, AggregateSimpleTest) {
  // Check if nested select columns are correctly processed
  LOG_INFO("Checking simple aggregate select.");

  std::string selectSQL = "SELECT MAX(b1) FROM B;";
  auto parse_tree = parser_.BuildParseTree(selectSQL);
  auto selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree[0].get());
  binder_->BindNameToNode(selectStmt);
  EXPECT_EQ(0, selectStmt->GetDepth());

  auto agg_expr = dynamic_cast<parser::AggregateExpression *>(selectStmt->GetSelectColumns()[0].get());
  EXPECT_EQ(type::TypeId::INTEGER, agg_expr->GetReturnValueType());
  EXPECT_EQ(0, agg_expr->GetDepth());

  auto col_expr = dynamic_cast<const parser::ColumnValueExpression *>(agg_expr->GetChild(0).get());
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // B.b1
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_);            // B.b1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // B.b1; columns are indexed from 1
  EXPECT_EQ(0, col_expr->GetDepth());
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, AggregateComplexTest) {
  // Check if nested select columns are correctly processed
  LOG_INFO("Checking aggregate in subselect.");

  std::string selectSQL = "SELECT A.a1 FROM A WHERE A.a1 IN (SELECT MAX(b1) FROM B);";
  auto parse_tree = parser_.BuildParseTree(selectSQL);
  auto selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree[0].get());
  binder_->BindNameToNode(selectStmt);
  EXPECT_EQ(0, selectStmt->GetDepth());

  auto subquery = dynamic_cast<parser::SubqueryExpression *>(selectStmt->GetSelectCondition()->GetChild(1).get());
  auto subselect = dynamic_cast<parser::SelectStatement *>(subquery->GetSubselect().get());

  auto agg_expr = dynamic_cast<parser::AggregateExpression *>(subselect->GetSelectColumns()[0].get());
  EXPECT_EQ(type::TypeId::INTEGER, agg_expr->GetReturnValueType());
  EXPECT_EQ(1, agg_expr->GetDepth());

  auto col_expr = dynamic_cast<const parser::ColumnValueExpression *>(agg_expr->GetChild(0).get());
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // B.b1
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_);            // B.b1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // B.b1; columns are indexed from 1
  EXPECT_EQ(1, col_expr->GetDepth());
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, OperatorComplexTest) {
  // Check if nested select columns are correctly processed
  LOG_INFO("Checking if operator expressions are correctly parsed.");

  std::string selectSQL = "SELECT A.a1 FROM A WHERE 2 * A.a1 IN (SELECT b1+1 FROM B);";
  auto parse_tree = parser_.BuildParseTree(selectSQL);
  auto selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree[0].get());
  binder_->BindNameToNode(selectStmt);
  EXPECT_EQ(0, selectStmt->GetDepth());

  auto op_expr = dynamic_cast<parser::OperatorExpression *>(selectStmt->GetSelectCondition()->GetChild(0).get());
  EXPECT_EQ(type::TypeId::INTEGER, op_expr->GetReturnValueType());
  EXPECT_EQ(0, op_expr->GetDepth());

  auto col_expr = dynamic_cast<const parser::ColumnValueExpression *>(op_expr->GetChild(1).get());
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // a.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_);            // a.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // a.a1; columns are indexed from 1
  EXPECT_EQ(0, col_expr->GetDepth());
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());

  auto subquery = dynamic_cast<parser::SubqueryExpression *>(selectStmt->GetSelectCondition()->GetChild(1).get());
  auto subselect = dynamic_cast<parser::SelectStatement *>(subquery->GetSubselect().get());

  op_expr = dynamic_cast<parser::OperatorExpression *>(subselect->GetSelectColumns()[0].get());
  EXPECT_EQ(type::TypeId::INTEGER, op_expr->GetReturnValueType());
  EXPECT_EQ(1, op_expr->GetDepth());

  col_expr = dynamic_cast<const parser::ColumnValueExpression *>(op_expr->GetChild(0).get());
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // B.b1
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_);            // B.b1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // B.b1; columns are indexed from 1
  EXPECT_EQ(1, col_expr->GetDepth());
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, BindDepthTest) {
  // Test regular table name
  LOG_INFO("Parsing sql query");

  std::string selectSQL =
      "SELECT A.a1 FROM A WHERE A.a1 IN (SELECT b1 FROM B WHERE b1 = 2 AND "
      "b2 > (SELECT a1 FROM A WHERE a2 > 0)) AND EXISTS (SELECT b1 FROM B WHERE B.b1 = A.a1)";
  auto parse_tree = parser_.BuildParseTree(selectSQL);
  auto selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree[0].get());
  binder_->BindNameToNode(selectStmt);

  // Check select depthGetSelectCondition
  EXPECT_EQ(0, selectStmt->GetDepth());

  // Check select_list
  LOG_INFO("Checking select list");
  auto tv_expr = selectStmt->GetSelectColumns()[0].get();
  EXPECT_EQ(0, tv_expr->GetDepth());  // A.a1

  // Check Where clause
  LOG_INFO("Checking where clause");
  EXPECT_EQ(0, selectStmt->GetSelectCondition()->GetDepth());  // XXX AND YYY

  // A.a1 IN (SELECT b1 FROM B WHERE b1 = 2 AND b2 > (SELECT a1 FROM A WHERE a2 > 0))
  auto in_expr = selectStmt->GetSelectCondition()->GetChild(0);  // A compare_in expression
  EXPECT_EQ(0, in_expr->GetDepth());

  auto in_tv_expr = in_expr->GetChild(0);  // A.a1
  EXPECT_EQ(0, in_tv_expr->GetDepth());

  // subquery expression
  auto in_sub_expr = in_expr->GetChild(1);
  EXPECT_EQ(1, in_sub_expr->GetDepth());
  EXPECT_TRUE(in_sub_expr->HasSubquery());

  // SELECT b1 FROM B WHERE b1 = 2 AND b2 > (SELECT a1 FROM A WHERE a2 > 0)
  auto in_sub_expr_select = dynamic_cast<parser::SubqueryExpression *>(in_sub_expr.get())->GetSubselect();
  EXPECT_EQ(1, in_sub_expr_select->GetDepth());

  auto in_sub_expr_select_ele = in_sub_expr_select->GetSelectColumns()[0].get();  // b1
  EXPECT_EQ(1, in_sub_expr_select_ele->GetDepth());

  // WHERE b1 = 2 AND b2 > (SELECT a1 FROM A WHERE a2 > 0)
  auto in_sub_expr_select_where = in_sub_expr_select->GetSelectCondition().get();
  EXPECT_EQ(1, in_sub_expr_select_where->GetDepth());

  // b1 = 2
  auto in_sub_expr_select_where_left = in_sub_expr_select_where->GetChild(0);
  EXPECT_EQ(1, in_sub_expr_select_where_left->GetDepth());

  // b2 > (SELECT a1 FROM A WHERE a2 > 0)
  auto in_sub_expr_select_where_right = in_sub_expr_select_where->GetChild(1);
  EXPECT_EQ(1, in_sub_expr_select_where_right->GetDepth());

  auto in_sub_expr_select_where_right_tv = in_sub_expr_select_where_right->GetChild(0);  // b2
  EXPECT_EQ(1, in_sub_expr_select_where_right_tv->GetDepth());

  // a subquery expression
  auto in_sub_expr_select_where_right_sub = in_sub_expr_select_where_right->GetChild(1);
  EXPECT_EQ(2, in_sub_expr_select_where_right_sub->GetDepth());
  EXPECT_TRUE(in_sub_expr->HasSubquery());

  // SELECT a1 FROM A WHERE a2 > 0
  auto in_sub_expr_select_where_right_sub_select =
      dynamic_cast<parser::SubqueryExpression *>(in_sub_expr_select_where_right_sub.get())->GetSubselect();
  EXPECT_EQ(2, in_sub_expr_select_where_right_sub_select->GetDepth());

  // WHERE a2 > 0
  auto in_sub_expr_select_where_right_sub_select_where =
      in_sub_expr_select_where_right_sub_select->GetSelectCondition().get();
  EXPECT_EQ(2, in_sub_expr_select_where_right_sub_select_where->GetDepth());

  // a1
  auto in_sub_expr_select_where_right_sub_select_ele =
      in_sub_expr_select_where_right_sub_select->GetSelectColumns()[0].get();
  EXPECT_EQ(2, in_sub_expr_select_where_right_sub_select_ele->GetDepth());

  // EXISTS (SELECT b1 FROM B WHERE B.b1 = A.a1)
  auto exists_expr = selectStmt->GetSelectCondition()->GetChild(1);  // An operator_exists expression
  EXPECT_EQ(0, exists_expr->GetDepth());

  // a subquery expression
  auto exists_sub_expr = exists_expr->GetChild(0);
  EXPECT_EQ(0, exists_sub_expr->GetDepth());
  EXPECT_TRUE(in_sub_expr->HasSubquery());

  // a select statement: SELECT b1 FROM B WHERE B.b1 = A.a1
  auto exists_sub_expr_select = dynamic_cast<parser::SubqueryExpression *>(exists_sub_expr.get())->GetSubselect();
  EXPECT_EQ(1, exists_sub_expr_select->GetDepth());

  // WHERE B.b1 = A.a1
  auto exists_sub_expr_select_where = exists_sub_expr_select->GetSelectCondition().get();
  EXPECT_EQ(0, exists_sub_expr_select_where->GetDepth());  // comparison expression take the highest level of depth; 0

  // b1 refer to column on the same level as the subselect
  auto exists_sub_expr_select_where_left =
      dynamic_cast<const parser::ColumnValueExpression *>(exists_sub_expr_select_where->GetChild(0).get());
  EXPECT_EQ(1, exists_sub_expr_select_where_left->GetDepth());

  // a1 refer to column on subselect's upper level
  auto exists_sub_expr_select_where_right =
      dynamic_cast<const parser::ColumnValueExpression *>(exists_sub_expr_select_where->GetChild(1).get());
  EXPECT_EQ(0, exists_sub_expr_select_where_right->GetDepth());

  auto exists_sub_expr_select_ele = exists_sub_expr_select->GetSelectColumns()[0].get();  // b1
  EXPECT_EQ(1, exists_sub_expr_select_ele->GetDepth());
}

// NOLINTNEXTLINE
// TEST_F(BinderCorrectnessTest, FunctionExpressionTest) {
//  std::string function_sql = "SELECT substr('test123', a, 3)";
//  auto parse_tree = parser_.BuildParseTree(function_sql);
//  auto stmt = parse_tree[0].get();
//  // TODO(ling): figure out specific exception that would be thrown
//  EXPECT_THROW(binder_->BindNameToNode(stmt), terrier::Exception);
//
//  function_sql = "SELECT substr('test123', 2, 3)";
//  auto parse_tree2 = parser_.BuildParseTree(function_sql);
//  stmt = parse_tree2[0].get();
//  binder_->BindNameToNode(stmt);
//  auto funct_expr = dynamic_cast<parser::FunctionExpression *>(dynamic_cast<parser::SelectStatement
//  *>(stmt)->select_list[0].get()); EXPECT_TRUE(funct_expr->Evaluate(nullptr, nullptr,
//  nullptr).CompareEquals(type::ValueFactory::GetVarcharValue("est")) == CmpBool::CmpTrue);
//}
}  // namespace terrier
