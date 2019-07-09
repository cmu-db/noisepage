#include <memory>
#include "util/transaction_benchmark_util.h"
#include "transaction/transaction_manager.h"
#include "util/test_harness.h"
#include "binder/bind_node_visitor.h"
#include "catalog/catalog.h"
#include "traffic_cop/statement.h"
#include "storage/garbage_collector.h"
#include "expression/subquery_expression.h"
#include "parser/expression/column_value_expression.h"
#include "optimizer/optimizer.h"
#include "parser/postgresparser.h"
#include "traffic_cop/traffic_cop.h"

#include "executor/testing_executor_util.h"
#include "sql/testing_sql_util.h"
#include "type/value_factory.h"

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
  catalog::db_oid_t db_oid_;
  std::string default_ns_name_ = "test_ns";
  catalog::namespace_oid_t ns_oid_;
  storage::GarbageCollector *gc_;
 protected:
  std::string default_database_name_ = "test_db";
  catalog::table_oid_t table_a_oid_;
  catalog::table_oid_t table_b_oid_;
  parser::PostgresParser parser_;
  transaction::TransactionManager txn_manager_{&buffer_pool_, true, LOGGING_DISABLED};
  transaction::TransactionContext* txn_;
  catalog::CatalogAccessor *accessor_;
  unique_ptr<binder::BindNodeVisitor> binder_;

  void flush() {
    // Run the GC to flush it down to a clean system
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
  }
  virtual void SetUp() override {
    TerrierTest::SetUp();

    gc_ = new storage::GarbageCollector(&txn_manager_);
    txn_ = txn_manager_.BeginTransaction();
    // new catalog requires txn_manage and block_store as parameters
    catalog_ = new catalog::Catalog(&txn_manager_, &block_store_);
    txn_manager_.Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
    flush();

    // create database
    txn_ = txn_manager_.BeginTransaction();
    LOG_INFO("Creating database %s", default_database_name_.c_str());
    db_oid_ = catalog_->CreateDatabase(txn_, default_database_name_);
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

    // create table A
    txn_ = txn_manager_.BeginTransaction();
    accessor_ = catalog_->GetAccessor(txn_, db_oid_);
    // Create the column definition (no OIDs) for CREATE TABLE A(A1 int, a2 varchar)
    std::vector<catalog::Schema::Column> cols_a;
    cols_a.emplace_back("A1", type::TypeId::INTEGER, true);
    cols_a.emplace_back("a2", type::TypeId::VARCHAR, true);
    auto schema_a = new catalog::Schema(cols_a);

    table_a_oid_ = accessor_->CreateTable(ns_oid_, "A", schema_a);
    txn_manager_.Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
    flush();

    // create Table B
    txn_ = txn_manager_.BeginTransaction();
    accessor_ = catalog_->GetAccessor(txn_, db_oid_);
    // Create the column definition (no OIDs) for CREATE TABLE b(b1 int, B2 varchar)
    std::vector<catalog::Schema::Column> cols_b;
    cols_b.emplace_back("b1", type::TypeId::INTEGER, true);
    cols_b.emplace_back("B2", type::TypeId::VARCHAR, true);
    auto schema_b = new catalog::Schema(cols_b);
    table_b_oid_ = accessor_->CreateTable(ns_oid_, "b", schema_b);
    txn_manager_.Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
    flush();

    delete schema_a;
    delete schema_b;

    // prepare for testing
    txn_ = txn_manager_.BeginTransaction();
    accessor_ = catalog_->GetAccessor(txn_, db_oid_);
    binder_ = std::make_unique<binder::BindNodeVisitor>(new binder::BindNodeVisitor(accessor_, default_database_name_));
  }

  virtual void TearDown() override {
    TerrierTest::TearDown();

    txn_manager_.Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
    // Delete the test database
    txn_ = txn_manager_.BeginTransaction();
    accessor_->DropDatabase(db_oid_);
    txn_manager_.Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
    flush();
    delete gc_;
    delete catalog_;
  }
};

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, SelectStatementTest) {
  // Test regular table name
  LOG_INFO("Parsing sql query");

  std::string selectSQL = "SELECT A.a1, B.b2 FROM A INNER JOIN b ON a.a1 = b.b1 WHERE a1 < 100 "
                     "GROUP BY A.a1, B.b2 HAVING a1 > 50 ORDER BY a1";

  auto parse_tree = parser_.BuildParseTree(selectSQL);
  auto selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree[0].get());
  binder_->BindNameToNode(selectStmt);

  auto db_oid = accessor_->GetDatabaseOid(default_database_name_);
  auto tableA_oid = accessor_->GetTableOid("a");
  auto tableB_oid = accessor_->GetTableOid("b");
  txn_manager_.Commit(txn_, TestCallbacks::EmptyCallback, nullptr);

  // Check select_list
  LOG_INFO("Checking select list");
  auto col_expr = dynamic_cast<const parser::ColumnValueExpression *>(selectStmt->GetSelectColumns()[0].get());
//  EXPECT_EQ(col_expr->GetBoundOid(), make_tuple(db_oid, tableA_oid, 0));  // A.a1
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid); //A.a1
  EXPECT_EQ(col_expr->GetTableOid(), tableA_oid); //A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1)); // A.a1; columns are indexed from 1

  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
  col_expr = (parser::ColumnValueExpression *)selectStmt->GetSelectColumns()[1].get();
//  EXPECT_EQ(col_expr->GetBoundOid(), make_tuple(db_oid, tableB_oid, 1));  // B.b2
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid); //B.b2
  EXPECT_EQ(col_expr->GetTableOid(), tableB_oid); //B.b2
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(2)); // B.b2; columns are indexed from 1

  EXPECT_EQ(type::TypeId::VARCHAR, col_expr->GetReturnValueType());

  // Check join condition
  LOG_INFO("Checking join condition");
  col_expr = (parser::ColumnValueExpression *)selectStmt->GetSelectTable()->GetJoin()->GetJoinCondition()->GetChild(0).get();
//  EXPECT_EQ(col_expr->GetBoundOid(), make_tuple(db_oid, tableA_oid, 0));  // a.a1
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid); //A.a1
  EXPECT_EQ(col_expr->GetTableOid(), tableA_oid); //A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1)); // A.a1; columns are indexed from 1

  col_expr = (parser::ColumnValueExpression *)selectStmt->GetSelectTable()->GetJoin()->GetJoinCondition()->GetChild(1).get();

//  EXPECT_EQ(col_expr->GetBoundOid(), make_tuple(db_oid, tableB_oid, 0));  // b.b1
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid); //B.b1
  EXPECT_EQ(col_expr->GetTableOid(), tableB_oid); //B.b1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1)); // B.b1; columns are indexed from 1
  
  // Check Where clause
  LOG_INFO("Checking where clause");
  col_expr = (parser::ColumnValueExpression *)selectStmt->GetSelectCondition()->GetChild(0).get();
//  EXPECT_EQ(col_expr->GetBoundOid(), make_tuple(db_oid, tableA_oid, 0));  // a1
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid); //A.a1
  EXPECT_EQ(col_expr->GetTableOid(), tableA_oid); //A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1)); // A.a1; columns are indexed from 1
  
  // Check Group By and Having
  LOG_INFO("Checking group by");
  col_expr = (parser::ColumnValueExpression *)selectStmt->GetSelectGroupBy()->GetColumns()[0].get();
//  EXPECT_EQ(col_expr->GetBoundOid(), make_tuple(db_oid, tableA_oid, 0));  // A.a1
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid); //A.a1
  EXPECT_EQ(col_expr->GetTableOid(), tableA_oid); //A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1)); // A.a1; columns are indexed from 1
  
  col_expr = dynamic_cast<const parser::ColumnValueExpression *>(selectStmt->GetSelectGroupBy()->GetColumns()[1].get());
//  EXPECT_EQ(col_expr->GetBoundOid(), make_tuple(db_oid, tableB_oid, 1));  // B.b2
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid); //B.b2
  EXPECT_EQ(col_expr->GetTableOid(), tableB_oid); //B.b2
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(2)); // B.b2; columns are indexed from 1
  
  col_expr = (parser::ColumnValueExpression *)selectStmt->GetSelectGroupBy()->GetHaving()->GetChild(0).get();
//  EXPECT_EQ(col_expr->GetBoundOid(), make_tuple(db_oid, tableA_oid, 0));  // a1
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid); //A.a1
  EXPECT_EQ(col_expr->GetTableOid(), tableA_oid); //A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1)); // A.a1; columns are indexed from 1
  
  // Check Order By
  LOG_INFO("Checking order by");
  col_expr = (parser::ColumnValueExpression *)selectStmt->GetSelectOrderBy()->GetOrderByExpressions()[0].get();
//  EXPECT_EQ(col_expr->GetBoundOid(), make_tuple(db_oid, tableA_oid, 0));  // a1
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid); //A.a1
  EXPECT_EQ(col_expr->GetTableOid(), tableA_oid); //A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1)); // A.a1; columns are indexed from 1
  
  // Check alias ambiguous
  LOG_INFO("Checking duplicate alias and table name.");

  txn_ = txn_manager_.BeginTransaction();
  binder_.reset(new binder::BindNodeVisitor(accessor_, default_database_name_));
  selectSQL = "SELECT * FROM A, B as A";
  parse_tree = parser_.BuildParseTree(selectSQL);
  selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree[0].get());
  try {
    binder_->BindNameToNode(selectStmt);
    EXPECT_TRUE(false);
  } catch (Exception &e) {
    LOG_INFO("Correct! Exception(%s) catched", e.what());
  }

  // Test select from different table instances from the same physical schema
  txn_manager_.Commit(txn_, TestCallbacks::EmptyCallback, nullptr);

  txn_ = txn_manager_.BeginTransaction();
  binder_.reset(new binder::BindNodeVisitor(accessor_, default_database_name_));
  selectSQL = "SELECT * FROM A, A as AA where A.a1 = AA.a2";
  parse_tree = parser_.BuildParseTree(selectSQL);
  selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree[0].get());
  binder_->BindNameToNode(selectStmt);
  LOG_INFO("Checking where clause");
  col_expr = dynamic_cast<const parser::ColumnValueExpression *>(selectStmt->GetSelectCondition()->GetChild(0).get());
//  EXPECT_EQ(col_expr->GetBoundOid(), make_tuple(db_oid, tableA_oid, 0));  // a1
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid); //A.a1
  EXPECT_EQ(col_expr->GetTableOid(), tableA_oid); //A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1)); // A.a1; columns are indexed from 1
  
  col_expr = dynamic_cast<const parser::ColumnValueExpression *>(selectStmt->GetSelectCondition()->GetChild(1).get());
//  EXPECT_EQ(col_expr->GetBoundOid(), make_tuple(db_oid, tableA_oid, 1));  // a1
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid); //A.a1
  EXPECT_EQ(col_expr->GetTableOid(), tableA_oid); //A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1)); // A.a1; columns are indexed from 1
  
  // Test alias and select_list
  LOG_INFO("Checking select_list and table alias binding");
  txn_manager_.Commit(txn_, TestCallbacks::EmptyCallback, nullptr);

  txn_ = txn_manager_.BeginTransaction();
  binder_.reset(new binder::BindNodeVisitor(accessor_, default_database_name_));
  selectSQL = "SELECT AA.a1, b2 FROM A as AA, B WHERE AA.a1 = B.b1";
  parse_tree = parser_.BuildParseTree(selectSQL);
  selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree[0].get());
  binder_->BindNameToNode(selectStmt);
  col_expr = dynamic_cast<parser::ColumnValueExpression *>(selectStmt->GetSelectColumns()[0].get());
//  EXPECT_EQ(col_expr->GetBoundOid(), make_tuple(db_oid, tableA_oid, 0)); // A.a1
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid); //A.a1
  EXPECT_EQ(col_expr->GetTableOid(), tableA_oid); //A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1)); // A.a1; columns are indexed from 1
  
  col_expr = dynamic_cast<parser::ColumnValueExpression *>(selectStmt->GetSelectColumns()[1].get());
//  EXPECT_EQ(col_expr->GetBoundOid(), make_tuple(db_oid, tableB_oid, 1)); // B.b2
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid); //B.b2
  EXPECT_EQ(col_expr->GetTableOid(), tableB_oid); //B.b2
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(2)); // B.b2; columns are indexed from 1
}

// TODO: add test for Update Statement. Currently UpdateStatement uses char*
// instead of ColumnValueExpression to represent column. We can only add this
// test after UpdateStatement is changed

TEST_F(BinderCorrectnessTest, DeleteStatementTest) {
  std::string default_database_name_ = "test_db";
  SetupTables(default_database_name_);
  auto &parser = parser::PostgresParser::GetInstance();
  catalog::Catalog *catalog_ptr = catalog::Catalog::GetInstance();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn_ = txn_manager.BeginTransaction();
  oid_t db_oid =
      catalog_ptr->GetDatabaseWithName(txn_, default_database_name_)->GetOid();
  oid_t tableB_oid = catalog_ptr
      ->GetTableWithName(txn_, default_database_name_, DEFAULT_SCHEMA_NAME, "b")
      ->GetOid();

  std::string deleteSQL = "DELETE FROM b WHERE 1 = b1 AND b2 = 'str'";
  unique_ptr<binder::BindNodeVisitor> binder(
      new binder::BindNodeVisitor(txn_, default_database_name_));

  auto parse_tree = parser_.BuildParseTree(deleteSQL);
  auto deleteStmt = dynamic_cast<parser::DeleteStatement *>(
      parse_tree->GetStatements().at(0).get());
  binder_->BindNameToNode(deleteStmt);

  txn_manager.Commit(txn_, TestCallbacks::EmptyCallback, nullptr);

  LOG_INFO("Checking first condition in where clause");
  auto col_expr = dynamic_cast<const parser::ColumnValueExpression *>(
      deleteStmt->expr->GetChild(0)->GetChild(1));
  EXPECT_EQ(col_expr->GetBoundOid(), make_tuple(db_oid, tableB_oid, 0));

  LOG_INFO("Checking second condition in where clause");
  col_expr = dynamic_cast<const parser::ColumnValueExpression *>(
      deleteStmt->expr->GetChild(1)->GetChild(0));
  EXPECT_EQ(col_expr->GetBoundOid(), make_tuple(db_oid, tableB_oid, 1));

  // Delete the test database
  txn_ = txn_manager.BeginTransaction();
  catalog_ptr->DropDatabaseWithName(txn_, default_database_name_);
  txn_manager.Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
}

TEST_F(BinderCorrectnessTest, BindDepthTest) {
std::string default_database_name_ = "test_db";
SetupTables(default_database_name_);
auto &parser = parser::PostgresParser::GetInstance();

// Test regular table name
LOG_INFO("Parsing sql query");

auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
auto txn_ = txn_manager.BeginTransaction();
unique_ptr<binder::BindNodeVisitor> binder(
    new binder::BindNodeVisitor(txn_, default_database_name_));
std::string selectSQL =
    "SELECT A.a1 FROM A WHERE A.a1 IN (SELECT b1 FROM B WHERE b1 = 2 AND b2 "
    "> (SELECT a1 FROM A WHERE a2 > 0)) "
    "AND EXISTS (SELECT b1 FROM B WHERE B.b1 = A.a1)";

auto parse_tree = parser_.BuildParseTree(selectSQL);
auto selectStmt = dynamic_cast<parser::SelectStatement *>(
    parse_tree->GetStatements().at(0).get());
binder_->BindNameToNode(selectStmt);
txn_manager.Commit(txn_, TestCallbacks::EmptyCallback, nullptr);

// Check select depth
EXPECT_EQ(0, selectStmt->depth);

// Check select_list
LOG_INFO("Checking select list");
auto tv_expr = selectStmt->select_list[0].get();
EXPECT_EQ(0, tv_expr->GetDepth());  // A.a1

// Check Where clause
LOG_INFO("Checking where clause");
EXPECT_EQ(0, selectStmt->where_clause->GetDepth());
auto in_expr = selectStmt->where_clause->GetChild(0);
auto exists_expr = selectStmt->where_clause->GetChild(1);
auto exists_sub_expr = exists_expr->GetChild(0);
auto exists_sub_expr_select =
    dynamic_cast<const parser::SubqueryExpression *>(exists_sub_expr)
        ->GetSubSelect();
auto exists_sub_expr_select_where =
    exists_sub_expr_select->where_clause.get();
auto exists_sub_expr_select_ele =
    exists_sub_expr_select->select_list[0].get();
auto in_tv_expr = in_expr->GetChild(0);
auto in_sub_expr = in_expr->GetChild(1);
auto in_sub_expr_select =
    dynamic_cast<const parser::SubqueryExpression *>(in_sub_expr)
        ->GetSubSelect();
auto in_sub_expr_select_where = in_sub_expr_select->where_clause.get();
auto in_sub_expr_select_ele = in_sub_expr_select->select_list[0].get();
auto in_sub_expr_select_where_left = in_sub_expr_select_where->GetChild(0);
auto in_sub_expr_select_where_right = in_sub_expr_select_where->GetChild(1);
auto in_sub_expr_select_where_right_tv =
    in_sub_expr_select_where_right->GetChild(0);
auto in_sub_expr_select_where_right_sub =
    in_sub_expr_select_where_right->GetChild(1);
auto in_sub_expr_select_where_right_sub_select =
    dynamic_cast<const parser::SubqueryExpression *>(
        in_sub_expr_select_where_right_sub)
        ->GetSubSelect();
auto in_sub_expr_select_where_right_sub_select_where =
    in_sub_expr_select_where_right_sub_select->where_clause.get();
auto in_sub_expr_select_where_right_sub_select_ele =
    in_sub_expr_select_where_right_sub_select->select_list[0].get();
EXPECT_EQ(0, in_expr->GetDepth());
EXPECT_EQ(0, exists_expr->GetDepth());
EXPECT_EQ(0, exists_sub_expr->GetDepth());
EXPECT_EQ(1, exists_sub_expr_select->depth);
EXPECT_EQ(0, exists_sub_expr_select_where->GetDepth());
EXPECT_EQ(1, exists_sub_expr_select_ele->GetDepth());
EXPECT_EQ(0, in_tv_expr->GetDepth());
EXPECT_EQ(1, in_sub_expr->GetDepth());
EXPECT_EQ(1, in_sub_expr_select->depth);
EXPECT_EQ(1, in_sub_expr_select_where->GetDepth());
EXPECT_EQ(1, in_sub_expr_select_ele->GetDepth());
EXPECT_EQ(1, in_sub_expr_select_where_left->GetDepth());
EXPECT_EQ(1, in_sub_expr_select_where_right->GetDepth());
EXPECT_EQ(1, in_sub_expr_select_where_right_tv->GetDepth());
EXPECT_EQ(2, in_sub_expr_select_where_right_sub->GetDepth());
EXPECT_EQ(2, in_sub_expr_select_where_right_sub_select->depth);
EXPECT_EQ(2, in_sub_expr_select_where_right_sub_select_where->GetDepth());
EXPECT_EQ(2, in_sub_expr_select_where_right_sub_select_ele->GetDepth());
// Delete the test database
catalog::Catalog *catalog_ptr = catalog::Catalog::GetInstance();
txn_ = txn_manager.BeginTransaction();
catalog_ptr->DropDatabaseWithName(txn_, default_database_name_);
txn_manager.Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
}

TEST_F(BinderCorrectnessTest, FunctionExpressionTest) {
auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
auto txn_ = txn_manager.BeginTransaction();

std::string function_sql = "SELECT substr('test123', a, 3)";
auto &parser = parser::PostgresParser::GetInstance();
auto parse_tree = parser_.BuildParseTree(function_sql);
auto stmt = parse_tree->GetStatement(0);
unique_ptr<binder::BindNodeVisitor> binder(
    new binder::BindNodeVisitor(txn_, DEFAULT_DB_NAME));
EXPECT_THROW(binder_->BindNameToNode(stmt), peloton::Exception);

function_sql = "SELECT substr('test123', 2, 3)";
auto parse_tree2 = parser_.BuildParseTree(function_sql);
stmt = parse_tree2->GetStatement(0);
binder_->BindNameToNode(stmt);
auto funct_expr = dynamic_cast<parser::FunctionExpression *>(
    dynamic_cast<parser::SelectStatement *>(stmt)->select_list[0].get());
EXPECT_TRUE(funct_expr->Evaluate(nullptr, nullptr, nullptr)
.CompareEquals(type::ValueFactory::GetVarcharValue("est")) ==
CmpBool::CmpTrue);

txn_manager.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
}

}  // namespace peloton
