#include <memory>
#include <string>
#include <vector>

#include "binder/bind_node_visitor.h"
#include "catalog/catalog.h"
#include "catalog/postgres/pg_proc.h"
#include "loggers/binder_logger.h"
#include "main/db_main.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/comparison_expression.h"
#include "parser/expression/function_expression.h"
#include "parser/expression/operator_expression.h"
#include "parser/expression/subquery_expression.h"
#include "parser/postgresparser.h"
#include "parser/statements.h"
#include "storage/sql_table.h"
#include "test_util/test_harness.h"
#include "transaction/transaction_manager.h"

using std::make_tuple;

using std::unique_ptr;
using std::vector;

namespace noisepage {

class BinderCorrectnessTest : public TerrierTest {
 protected:
  std::string default_database_name_ = "test_db";
  catalog::db_oid_t db_oid_;
  catalog::table_oid_t table_a_oid_;
  catalog::table_oid_t table_b_oid_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  transaction::TransactionContext *txn_;
  std::unique_ptr<catalog::CatalogAccessor> accessor_;
  binder::BindNodeVisitor *binder_;

  std::unique_ptr<DBMain> db_main_;

  void SetUpTables() {
    // create database
    txn_ = txn_manager_->BeginTransaction();
    BINDER_LOG_DEBUG("Creating database %s", default_database_name_.c_str());
    db_oid_ = catalog_->CreateDatabase(common::ManagedPointer(txn_), default_database_name_, true);
    // commit the transactions
    txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
    BINDER_LOG_DEBUG("database %s created!", default_database_name_.c_str());

    // get default values of the columns
    auto int_default = parser::ConstantValueExpression(type::TypeId::INTEGER);
    auto varchar_default = parser::ConstantValueExpression(type::TypeId::VARCHAR);

    // TODO(Ling): use mixed case in table name and schema name
    //  for testcases to see if the binder does not differentiate between upper and lower cases
    // create table A
    txn_ = txn_manager_->BeginTransaction();
    accessor_ = catalog_->GetAccessor(common::ManagedPointer(txn_), db_oid_, DISABLED);
    // Create the column definition (no OIDs) for CREATE TABLE A(A1 int, a2 varchar)
    std::vector<catalog::Schema::Column> cols_a;
    cols_a.emplace_back("a1", type::TypeId::INTEGER, true, int_default);
    cols_a.emplace_back("a2", type::TypeId::VARCHAR, 20, true, varchar_default);
    auto schema_a = catalog::Schema(cols_a);

    table_a_oid_ = accessor_->CreateTable(accessor_->GetDefaultNamespace(), "a", schema_a);
    auto table_a = new storage::SqlTable(db_main_->GetStorageLayer()->GetBlockStore(), schema_a);
    EXPECT_TRUE(accessor_->SetTablePointer(table_a_oid_, table_a));

    txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
    accessor_.reset(nullptr);

    // create Table B
    txn_ = txn_manager_->BeginTransaction();
    accessor_ = catalog_->GetAccessor(common::ManagedPointer(txn_), db_oid_, DISABLED);

    // Create the column definition (no OIDs) for CREATE TABLE b(b1 int, B2 varchar)
    std::vector<catalog::Schema::Column> cols_b;
    cols_b.emplace_back("b1", type::TypeId::INTEGER, true, int_default);
    cols_b.emplace_back("b2", type::TypeId::VARCHAR, 20, true, varchar_default);

    auto schema_b = catalog::Schema(cols_b);
    table_b_oid_ = accessor_->CreateTable(accessor_->GetDefaultNamespace(), "b", schema_b);
    auto table_b = new storage::SqlTable(db_main_->GetStorageLayer()->GetBlockStore(), schema_b);
    EXPECT_TRUE(accessor_->SetTablePointer(table_b_oid_, table_b));
    txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
    accessor_.reset(nullptr);
  }

  void SetUp() override {
    db_main_ = noisepage::DBMain::Builder().SetUseGC(true).SetUseCatalog(true).Build();
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
    catalog_ = db_main_->GetCatalogLayer()->GetCatalog();

    SetUpTables();
    // prepare for testing
    txn_ = txn_manager_->BeginTransaction();
    accessor_ = catalog_->GetAccessor(common::ManagedPointer(txn_), db_oid_, DISABLED);
    binder_ = new binder::BindNodeVisitor(common::ManagedPointer(accessor_), db_oid_);
  }

  void TearDown() override {
    txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
    accessor_.reset(nullptr);
    delete binder_;
  }
};

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, SelectStatementInvalidTableTest) {
  // Test regular table name
  BINDER_LOG_DEBUG("Parsing sql query");
  std::string select_sql = "SELECT a1 FROM c;";

  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  EXPECT_THROW(binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr), BinderException);
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, SelectStatementInvalidColumnTest) {
  // Test regular table name
  BINDER_LOG_DEBUG("Parsing sql query");
  std::string select_sql = "SELECT a8 FROM a;";

  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  EXPECT_THROW(binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr), BinderException);
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, SelectStatementComplexTest) {
  // Test regular table name
  BINDER_LOG_DEBUG("Parsing sql query");
  std::string select_sql =
      "SELECT A.A1, B.B2 FROM A INNER JOIN b ON a.a1 = b.b1 WHERE a1 < 100 "
      "GROUP BY A.a1, B.b2 HAVING a1 > 50 ORDER BY a1";

  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  auto select_stmt = statement.CastManagedPointerTo<parser::SelectStatement>();
  EXPECT_EQ(0, select_stmt->GetDepth());

  // Check select_list
  BINDER_LOG_DEBUG("Checking select list");
  auto col_expr = select_stmt->GetSelectColumns()[0].CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // A.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_);            // A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // A.a1; columns are indexed from 1
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
  EXPECT_EQ(0, col_expr->GetDepth());

  col_expr = select_stmt->GetSelectColumns()[1].CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // B.b2
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_);            // B.b2
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(2));  // B.b2; columns are indexed from 1
  EXPECT_EQ(type::TypeId::VARCHAR, col_expr->GetReturnValueType());
  EXPECT_EQ(0, col_expr->GetDepth());

  // Check join condition
  BINDER_LOG_DEBUG("Checking join condition");
  col_expr = select_stmt->GetSelectTable()
                 ->GetJoin()
                 ->GetJoinCondition()
                 ->GetChild(0)
                 .CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // A.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_);            // A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // A.a1; columns are indexed from 1
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
  EXPECT_EQ(0, col_expr->GetDepth());

  col_expr = select_stmt->GetSelectTable()
                 ->GetJoin()
                 ->GetJoinCondition()
                 ->GetChild(1)
                 .CastManagedPointerTo<parser::ColumnValueExpression>();

  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // B.b1
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_);            // B.b1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // B.b1; columns are indexed from 1
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
  EXPECT_EQ(0, col_expr->GetDepth());

  // Check Where clause
  BINDER_LOG_DEBUG("Checking where clause");
  col_expr = select_stmt->GetSelectCondition()->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // A.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_);            // A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // A.a1; columns are indexed from 1
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
  EXPECT_EQ(0, col_expr->GetDepth());

  // Check Group By and Having
  BINDER_LOG_DEBUG("Checking group by");
  col_expr = select_stmt->GetSelectGroupBy()->GetColumns()[0].CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // A.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_);            // A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // A.a1; columns are indexed from 1
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
  EXPECT_EQ(0, col_expr->GetDepth());

  col_expr = select_stmt->GetSelectGroupBy()->GetColumns()[1].CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // B.b2
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_);            // B.b2
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(2));  // B.b2; columns are indexed from 1
  EXPECT_EQ(type::TypeId::VARCHAR, col_expr->GetReturnValueType());
  EXPECT_EQ(0, col_expr->GetDepth());

  col_expr =
      select_stmt->GetSelectGroupBy()->GetHaving()->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // A.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_);            // A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // A.a1; columns are indexed from 1
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
  EXPECT_EQ(0, col_expr->GetDepth());

  // Check Order By
  BINDER_LOG_DEBUG("Checking order by");
  col_expr =
      select_stmt->GetSelectOrderBy()->GetOrderByExpressions()[0].CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // A.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_);            // A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // A.a1; columns are indexed from 1
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
  EXPECT_EQ(0, col_expr->GetDepth());
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, SelectStatementStarTest) {
  // Check if star expression is correctly processed
  BINDER_LOG_DEBUG("Checking STAR expression in select and sub-select");

  std::string select_sql = "SELECT * FROM A LEFT OUTER JOIN B ON A.A1 < B.B1";
  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  auto select_stmt = statement.CastManagedPointerTo<parser::SelectStatement>();
  EXPECT_EQ(0, select_stmt->GetDepth());

  // Check select_list
  BINDER_LOG_DEBUG("Checking select list expansion");

  auto columns = select_stmt->GetSelectColumns();
  EXPECT_EQ(columns.size(), 4);

  // check if all required columns exist regardless of order, are they come from traversing unordered maps

  bool a1_exists = false;
  bool a2_exists = false;
  bool b1_exists = false;
  bool b2_exists = false;
  for (auto &col_abs_expr : columns) {
    auto col_expr = col_abs_expr.CastManagedPointerTo<parser::ColumnValueExpression>();
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
  BINDER_LOG_DEBUG("Checking STAR expression in nested select from.");

  std::string select_sql =
      "SELECT * FROM A LEFT OUTER JOIN (SELECT * FROM B INNER JOIN A ON B1 = A1) AS C ON C.B1 = a.A1";
  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  auto select_stmt = statement.CastManagedPointerTo<parser::SelectStatement>();
  EXPECT_EQ(0, select_stmt->GetDepth());

  // Check select_list
  BINDER_LOG_DEBUG("Checking select list expansion");
  auto columns = select_stmt->GetSelectColumns();
  EXPECT_EQ(columns.size(), 6);

  // check if all required columns exist regardless of order, are they come from traversing unordered maps

  bool a1_exists = false;
  bool a2_exists = false;
  bool c_a1_exists = false;
  bool c_a2_exists = false;
  bool c_b1_exists = false;
  bool c_b2_exists = false;

  for (auto &col_abs_expr : columns) {
    auto col_expr = col_abs_expr.CastManagedPointerTo<parser::ColumnValueExpression>();
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
  BINDER_LOG_DEBUG("Checking join condition");
  auto col_expr = select_stmt->GetSelectTable()
                      ->GetJoin()
                      ->GetJoinCondition()
                      ->GetChild(0)
                      .CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col_expr->GetColumnName(), "b1");  // C.B1
  EXPECT_EQ(col_expr->GetTableName(), "c");
  EXPECT_EQ(col_expr->GetDatabaseOid(), catalog::INVALID_DATABASE_OID);
  EXPECT_EQ(col_expr->GetTableOid(), catalog::INVALID_TABLE_OID);
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::INVALID_COLUMN_OID);
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
  EXPECT_EQ(col_expr->GetDepth(), 0);  // not from derived subquery

  col_expr = select_stmt->GetSelectTable()
                 ->GetJoin()
                 ->GetJoinCondition()
                 ->GetChild(1)
                 .CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col_expr->GetColumnName(), "a1");                  // A.a1
  EXPECT_EQ(col_expr->GetTableName(), "a");                    // A.a1
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // A.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_);            // A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // A.a1; columns are indexed from 1
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
  EXPECT_EQ(col_expr->GetDepth(), 0);  // not from derived subquery

  // check right table
  BINDER_LOG_DEBUG("Checking nested table of the join");
  auto right_tb = select_stmt->GetSelectTable()
                      ->GetJoin()
                      ->GetRightTable()
                      ->GetSelect()
                      .CastManagedPointerTo<parser::SelectStatement>();
  EXPECT_EQ(right_tb->GetDepth(), 1);  // 1 level subselect
  BINDER_LOG_DEBUG("Checking nested column select list");

  columns = right_tb->GetSelectColumns();
  EXPECT_EQ(columns.size(), 4);

  // check if all required columns exist regardless of order, are they come from traversing unordered maps
  a1_exists = false;
  a2_exists = false;
  bool b1_exists = false;
  bool b2_exists = false;
  for (auto &col_abs_expr : columns) {
    col_expr = col_abs_expr.CastManagedPointerTo<parser::ColumnValueExpression>();
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

  BINDER_LOG_DEBUG("Checking nested table's join condition");
  auto join = right_tb->GetSelectTable()->GetJoin()->GetJoinCondition();
  // TODO(Ling): the join condition expression still has depth -1;
  //  but the left and right column value expressions have correct depth;
  //  Left for the optimizer to decide if depth of the `ON A1 = B1` is necessary to be set to 1

  col_expr = join->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // B.b1
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_);            // B.b1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // B.b1; columns are indexed from 1
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
  EXPECT_EQ(1, col_expr->GetDepth());

  col_expr = join->GetChild(1).CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // A.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_);            // A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // A.a1; columns are indexed from 1
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
  EXPECT_EQ(1, col_expr->GetDepth());
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, SelectStatementNestedColumnTest) {
  // Check if nested select columns are correctly processed
  BINDER_LOG_DEBUG("Checking nested select columns.");

  std::string select_sql = "SELECT A1, (SELECT B2 FROM B where B2 IS NULL LIMIT 1) FROM A";
  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  auto select_stmt = statement.CastManagedPointerTo<parser::SelectStatement>();
  EXPECT_EQ(0, select_stmt->GetDepth());

  // Check select_list
  BINDER_LOG_DEBUG("Checking select list");
  auto col_expr = select_stmt->GetSelectColumns()[0].CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // A.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_);            // A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // A.a1; columns are indexed from 1
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
  EXPECT_EQ(0, col_expr->GetDepth());

  BINDER_LOG_DEBUG("Checking nested select in select list");
  auto subquery = select_stmt->GetSelectColumns()[1].CastManagedPointerTo<parser::SubqueryExpression>();
  EXPECT_EQ(subquery->GetDepth(), 1);

  auto subselect = subquery->GetSubselect().CastManagedPointerTo<parser::SelectStatement>();
  EXPECT_EQ(subselect->GetDepth(), 1);

  BINDER_LOG_DEBUG("Checking select list in nested select in select list");
  col_expr = subselect->GetSelectColumns()[0].CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // B.b2
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_);            // B.b2
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(2));  // B.b2; columns are indexed from 1
  EXPECT_EQ(type::TypeId::VARCHAR, col_expr->GetReturnValueType());
  EXPECT_EQ(1, col_expr->GetDepth());

  BINDER_LOG_DEBUG("Checking where clause in nested select in select list");
  auto where = subselect->GetSelectCondition().CastManagedPointerTo<parser::OperatorExpression>();
  EXPECT_EQ(where->GetDepth(), 1);

  col_expr = where->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // B.b2
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_);            // B.b2
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(2));  // B.b2; columns are indexed from 1
  EXPECT_EQ(type::TypeId::VARCHAR, col_expr->GetReturnValueType());
  EXPECT_EQ(1, col_expr->GetDepth());
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, SelectStatementDupAliasTest) {
  // Check alias ambiguous
  BINDER_LOG_DEBUG("Checking duplicate alias and table name.");

  std::string select_sql = "SELECT * FROM A, B as A";
  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  EXPECT_THROW(binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr), BinderException);
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, SelectStatementDiffTableSameSchemaTest) {
  // Test select from different table instances from the same physical schema
  std::string select_sql = "SELECT * FROM A, A as AA where A.a1 = AA.a1";
  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  auto select_stmt = statement.CastManagedPointerTo<parser::SelectStatement>();
  BINDER_LOG_DEBUG("Checking where clause");
  auto col_expr = select_stmt->GetSelectCondition()->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // A.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_);            // A.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // A.a1; columns are indexed from 1

  col_expr = select_stmt->GetSelectCondition()->GetChild(1).CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // AA.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_);            // AA.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // AA.a1; columns are indexed from 1
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, SelectStatementSelectListAliasTest) {
  // Test alias and select_list
  BINDER_LOG_DEBUG("Checking select_list and table alias binding");

  std::string select_sql = "SELECT AA.a1, b2 FROM A as AA, B WHERE AA.a1 = B.b1";
  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  auto select_stmt = statement.CastManagedPointerTo<parser::SelectStatement>();
  auto col_expr = select_stmt->GetSelectColumns()[0].CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // AA.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_);            // AA.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // AA.a1; columns are indexed from 1

  col_expr = select_stmt->GetSelectColumns()[1].CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // b2
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_);            // b2
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(2));  // b2; columns are indexed from 1
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, UpdateStatementSimpleTest) {
  std::string update_sql = "UPDATE A SET A1 = 999 WHERE A1 >= 1";
  auto parse_tree = parser::PostgresParser::BuildParseTree(update_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  auto update_stmt = statement.CastManagedPointerTo<parser::UpdateStatement>();

  BINDER_LOG_DEBUG("Checking update clause");
  auto update_clause = update_stmt->GetUpdateClauses()[0].Get();
  EXPECT_EQ("a1", update_clause->GetColumnName());
  auto constant = update_clause->GetUpdateValue().CastManagedPointerTo<parser::ConstantValueExpression>();
  EXPECT_EQ(constant->GetReturnValueType(), type::TypeId::INTEGER);
  EXPECT_EQ(constant->Peek<int64_t>(), 999);

  BINDER_LOG_DEBUG("Checking update condition");
  auto col_expr = update_stmt->GetUpdateCondition()->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_);            // a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // a1; columns are indexed from 1
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, DeleteStatementWhereTest) {
  std::string delete_sql = "DELETE FROM b WHERE 1 = b1 AND b2 = 'str'";
  auto parse_tree = parser::PostgresParser::BuildParseTree(delete_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  auto delete_stmt = statement.CastManagedPointerTo<parser::DeleteStatement>();

  BINDER_LOG_DEBUG("Checking first condition in where clause");
  auto col_expr =
      delete_stmt->GetDeleteCondition()->GetChild(0)->GetChild(1).CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // b1
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_);            // b1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // b1; columns are indexed from 1

  BINDER_LOG_DEBUG("Checking second condition in where clause");
  col_expr =
      delete_stmt->GetDeleteCondition()->GetChild(1)->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // b2
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_);            // b2
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(2));  // b2; columns are indexed from 1
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, AggregateSimpleTest) {
  // Check if nested select columns are correctly processed
  BINDER_LOG_DEBUG("Checking simple aggregate select.");

  std::string select_sql = "SELECT MAX(b1) FROM B;";
  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  auto select_stmt = statement.CastManagedPointerTo<parser::SelectStatement>();
  EXPECT_EQ(0, select_stmt->GetDepth());

  auto agg_expr = select_stmt->GetSelectColumns()[0].CastManagedPointerTo<parser::AggregateExpression>();
  EXPECT_EQ(type::TypeId::INTEGER, agg_expr->GetReturnValueType());
  EXPECT_EQ(0, agg_expr->GetDepth());

  auto col_expr = agg_expr->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // B.b1
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_);            // B.b1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // B.b1; columns are indexed from 1
  EXPECT_EQ(0, col_expr->GetDepth());
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, AggregateComplexTest) {
  // Check if nested select columns are correctly processed
  BINDER_LOG_DEBUG("Checking aggregate in subselect.");

  std::string select_sql = "SELECT A.a1 FROM A WHERE A.a1 IN (SELECT MAX(b1) FROM B);";
  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  auto select_stmt = statement.CastManagedPointerTo<parser::SelectStatement>();
  EXPECT_EQ(0, select_stmt->GetDepth());

  auto subquery = select_stmt->GetSelectCondition()->GetChild(1).CastManagedPointerTo<parser::SubqueryExpression>();
  auto subselect = subquery->GetSubselect().CastManagedPointerTo<parser::SelectStatement>();

  auto agg_expr = subselect->GetSelectColumns()[0].CastManagedPointerTo<parser::AggregateExpression>();
  EXPECT_EQ(type::TypeId::INTEGER, agg_expr->GetReturnValueType());
  EXPECT_EQ(1, agg_expr->GetDepth());

  auto col_expr = agg_expr->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // B.b1
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_);            // B.b1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // B.b1; columns are indexed from 1
  EXPECT_EQ(1, col_expr->GetDepth());
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, OperatorComplexTest) {
  // Check if nested select columns are correctly processed
  BINDER_LOG_DEBUG("Checking if operator expressions are correctly parsed.");

  std::string select_sql = "SELECT A.a1 FROM A WHERE 2 * A.a1 IN (SELECT b1+1 FROM B);";
  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  auto select_stmt = statement.CastManagedPointerTo<parser::SelectStatement>();
  EXPECT_EQ(0, select_stmt->GetDepth());

  auto op_expr = select_stmt->GetSelectCondition()->GetChild(0).CastManagedPointerTo<parser::OperatorExpression>();
  EXPECT_EQ(type::TypeId::INTEGER, op_expr->GetReturnValueType());
  EXPECT_EQ(0, op_expr->GetDepth());

  auto col_expr = op_expr->GetChild(1).CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // a.a1
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_);            // a.a1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // a.a1; columns are indexed from 1
  EXPECT_EQ(0, col_expr->GetDepth());
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());

  auto subquery = select_stmt->GetSelectCondition()->GetChild(1).CastManagedPointerTo<parser::SubqueryExpression>();
  auto subselect = subquery->GetSubselect().CastManagedPointerTo<parser::SelectStatement>();

  op_expr = subselect->GetSelectColumns()[0].CastManagedPointerTo<parser::OperatorExpression>();
  EXPECT_EQ(type::TypeId::INTEGER, op_expr->GetReturnValueType());
  EXPECT_EQ(1, op_expr->GetDepth());

  col_expr = op_expr->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col_expr->GetDatabaseOid(), db_oid_);              // B.b1
  EXPECT_EQ(col_expr->GetTableOid(), table_b_oid_);            // B.b1
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));  // B.b1; columns are indexed from 1
  EXPECT_EQ(1, col_expr->GetDepth());
  EXPECT_EQ(type::TypeId::INTEGER, col_expr->GetReturnValueType());
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, BindDepthTest) {
  // Check if expressions in nested queries have the correct depth in the abstract syntax tree
  BINDER_LOG_DEBUG("Parsing sql query");

  std::string select_sql =
      "SELECT A.a1 FROM A WHERE A.a1 IN (SELECT b1 FROM B WHERE b1 = 2 AND "
      "b1 > (SELECT a1 FROM A WHERE a1 > 0)) AND EXISTS (SELECT b1 FROM B WHERE B.b1 = A.a1)";
  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  auto select_stmt = statement.CastManagedPointerTo<parser::SelectStatement>();

  // Check select depthGetSelectCondition
  EXPECT_EQ(0, select_stmt->GetDepth());

  // Check select_list
  BINDER_LOG_DEBUG("Checking select list");
  auto tv_expr = select_stmt->GetSelectColumns()[0];
  EXPECT_EQ(0, tv_expr->GetDepth());  // A.a1

  // Check Where clause
  BINDER_LOG_DEBUG("Checking where clause");
  EXPECT_EQ(0, select_stmt->GetSelectCondition()->GetDepth());  // XXX AND YYY

  // A.a1 IN (SELECT b1 FROM B WHERE b1 = 2 AND b1 > (SELECT a1 FROM A WHERE a1 > 0))
  auto in_expr = select_stmt->GetSelectCondition()->GetChild(0);  // A compare_in expression
  EXPECT_EQ(0, in_expr->GetDepth());

  auto in_tv_expr = in_expr->GetChild(0);  // A.a1
  EXPECT_EQ(0, in_tv_expr->GetDepth());

  // subquery expression
  auto in_sub_expr = in_expr->GetChild(1);
  EXPECT_EQ(1, in_sub_expr->GetDepth());
  EXPECT_TRUE(in_sub_expr->HasSubquery());

  // SELECT b1 FROM B WHERE b1 = 2 AND b1 > (SELECT a1 FROM A WHERE a1 > 0)
  auto in_sub_expr_select = in_sub_expr.CastManagedPointerTo<parser::SubqueryExpression>()->GetSubselect();
  EXPECT_EQ(1, in_sub_expr_select->GetDepth());

  auto in_sub_expr_select_ele = in_sub_expr_select->GetSelectColumns()[0];  // b1
  EXPECT_EQ(1, in_sub_expr_select_ele->GetDepth());

  // WHERE b1 = 2 AND b1 > (SELECT a1 FROM A WHERE a1 > 0)
  auto in_sub_expr_select_where = in_sub_expr_select->GetSelectCondition();
  EXPECT_EQ(1, in_sub_expr_select_where->GetDepth());

  // b1 = 2
  auto in_sub_expr_select_where_left = in_sub_expr_select_where->GetChild(0);
  EXPECT_EQ(1, in_sub_expr_select_where_left->GetDepth());

  // b1 > (SELECT a1 FROM A WHERE a1 > 0)
  auto in_sub_expr_select_where_right = in_sub_expr_select_where->GetChild(1);
  EXPECT_EQ(1, in_sub_expr_select_where_right->GetDepth());

  auto in_sub_expr_select_where_right_tv = in_sub_expr_select_where_right->GetChild(0);  // b1
  EXPECT_EQ(1, in_sub_expr_select_where_right_tv->GetDepth());

  // a subquery expression
  auto in_sub_expr_select_where_right_sub = in_sub_expr_select_where_right->GetChild(1);
  EXPECT_EQ(2, in_sub_expr_select_where_right_sub->GetDepth());
  EXPECT_TRUE(in_sub_expr->HasSubquery());

  // SELECT a1 FROM A WHERE a1 > 0
  auto in_sub_expr_select_where_right_sub_select =
      in_sub_expr_select_where_right_sub.CastManagedPointerTo<parser::SubqueryExpression>()->GetSubselect();
  EXPECT_EQ(2, in_sub_expr_select_where_right_sub_select->GetDepth());

  // WHERE a1 > 0
  auto in_sub_expr_select_where_right_sub_select_where =
      in_sub_expr_select_where_right_sub_select->GetSelectCondition();
  EXPECT_EQ(2, in_sub_expr_select_where_right_sub_select_where->GetDepth());

  // a1
  auto in_sub_expr_select_where_right_sub_select_ele = in_sub_expr_select_where_right_sub_select->GetSelectColumns()[0];
  EXPECT_EQ(2, in_sub_expr_select_where_right_sub_select_ele->GetDepth());

  // EXISTS (SELECT b1 FROM B WHERE B.b1 = A.a1)
  auto exists_expr = select_stmt->GetSelectCondition()->GetChild(1);  // An operator_exists expression
  EXPECT_EQ(0, exists_expr->GetDepth());

  // a subquery expression
  auto exists_sub_expr = exists_expr->GetChild(0);
  EXPECT_EQ(0, exists_sub_expr->GetDepth());
  EXPECT_TRUE(in_sub_expr->HasSubquery());

  // a select statement: SELECT b1 FROM B WHERE B.b1 = A.a1
  auto exists_sub_expr_select = exists_sub_expr.CastManagedPointerTo<parser::SubqueryExpression>()->GetSubselect();
  EXPECT_EQ(1, exists_sub_expr_select->GetDepth());

  // WHERE B.b1 = A.a1
  auto exists_sub_expr_select_where = exists_sub_expr_select->GetSelectCondition();
  EXPECT_EQ(0, exists_sub_expr_select_where->GetDepth());  // comparison expression take the highest level of depth; 0

  // b1 refer to column on the same level as the subselect
  auto exists_sub_expr_select_where_left = exists_sub_expr_select_where->GetChild(0);
  EXPECT_EQ(1, exists_sub_expr_select_where_left->GetDepth());

  // a1 refer to column on subselect's upper level
  auto exists_sub_expr_select_where_right = exists_sub_expr_select_where->GetChild(1);
  EXPECT_EQ(0, exists_sub_expr_select_where_right->GetDepth());

  auto exists_sub_expr_select_ele = exists_sub_expr_select->GetSelectColumns()[0];  // b1
  EXPECT_EQ(1, exists_sub_expr_select_ele->GetDepth());
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, CreateDatabaseTest) {
  // Check if nested select columns are correctly processed
  BINDER_LOG_DEBUG("Checking create database.");

  std::string create_sql = "CREATE DATABASE C;";
  auto parse_tree = parser::PostgresParser::BuildParseTree(create_sql);
  EXPECT_NO_THROW(binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr));
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, CreateTableTest) {
  BINDER_LOG_DEBUG("Checking create table. Note that CREATE AS from select is not supported.");

  std::string create_sql =
      "CREATE TABLE C ( C1 int NOT NULL, C2 varchar(255) NOT NULL UNIQUE, C3 INT REFERENCES A(A1), C4 INT DEFAULT 14 "
      "CHECK (C4<100), PRIMARY KEY(C1));";
  auto parse_tree = parser::PostgresParser::BuildParseTree(create_sql);
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);

  auto create_stmt = parse_tree->GetStatements()[0].CastManagedPointerTo<parser::CreateStatement>();
  EXPECT_TRUE(create_stmt != nullptr);
  auto check_expr = create_stmt->GetColumns().at(3)->GetCheckExpression();
  EXPECT_TRUE(check_expr != nullptr);
  auto comp_expr = check_expr.CastManagedPointerTo<parser::ComparisonExpression>();
  // check (c4 < 100)
  EXPECT_EQ(comp_expr->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>()->GetTableName(), "c");
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, CreateTableSimpleForeignTest) {
  BINDER_LOG_DEBUG("Checking create table foreign key");

  std::string create_sql =
      "CREATE TABLE D ( D1 int NOT NULL, D2 varchar(255) NOT NULL UNIQUE, D3 INT UNIQUE, D4 VARCHAR(20) UNIQUE, "
      "PRIMARY KEY(D1, D2), FOREIGN KEY(D3, D4) REFERENCES A(A1, A2));";
  auto parse_tree = parser::PostgresParser::BuildParseTree(create_sql);
  EXPECT_NO_THROW(binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr));
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, CreateTableSimpleForeignViolateTest) {
  BINDER_LOG_DEBUG("Checking create table foreign key type unmatch");

  std::string create_sql =
      "CREATE TABLE D ( D1 int NOT NULL, D2 varchar(255) NOT NULL UNIQUE, D3 INT UNIQUE, D4 INT UNIQUE, PRIMARY "
      "KEY(D1, D2), FOREIGN KEY(D4, D3) REFERENCES A(A1, A2));";
  auto parse_tree = parser::PostgresParser::BuildParseTree(create_sql);
  EXPECT_THROW(binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr), BinderException);
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, CreateIndexTest) {
  BINDER_LOG_DEBUG("Checking create index");

  std::string create_sql = "CREATE UNIQUE INDEX idx_d ON A (A2, A1);";
  auto parse_tree = parser::PostgresParser::BuildParseTree(create_sql);
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, CreateTriggerTest) {
  BINDER_LOG_DEBUG("Checking create trigger");

  std::string create_sql =
      "CREATE TRIGGER check_update "
      "BEFORE UPDATE OF a1 ON a "
      "FOR EACH ROW "
      "WHEN (OLD.a1 <> NEW.a1) "
      "EXECUTE PROCEDURE check_account_update(update_date);";
  auto parse_tree = parser::PostgresParser::BuildParseTree(create_sql);
  auto statement = parse_tree->GetStatements()[0];
  auto create_stmt = statement.CastManagedPointerTo<parser::CreateStatement>();
  EXPECT_NO_THROW(binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr));
  auto col1 = create_stmt->GetTriggerWhen()->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
  auto col2 = create_stmt->GetTriggerWhen()->GetChild(1).CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col1->GetTableOid(), table_a_oid_);
  EXPECT_EQ(col2->GetTableOid(), table_a_oid_);
  EXPECT_EQ(col1->GetColumnOid(), catalog::col_oid_t(1));
  EXPECT_EQ(col2->GetColumnOid(), catalog::col_oid_t(1));
  EXPECT_EQ(col1->GetDatabaseOid(), db_oid_);
  EXPECT_EQ(col2->GetDatabaseOid(), db_oid_);
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, CreateViewTest) {
  std::string create_sql = "CREATE VIEW a_view AS SELECT * FROM a WHERE a1 = 4;";

  auto parse_tree = parser::PostgresParser::BuildParseTree(create_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);

  // Test logical create
  auto create_stmt = statement.CastManagedPointerTo<parser::CreateStatement>();
  //  EXPECT_EQ(create_stmt->GetDatabaseName(), "test_db");
  auto view_query = create_stmt->GetViewQuery();
  //  EXPECT_EQ(view_query->GetSelectTable()->GetDatabaseName(), "test_db");
  EXPECT_EQ(view_query->GetSelectTable()->GetTableName(), "a");
  auto cond_expr = view_query->GetSelectCondition().CastManagedPointerTo<parser::ComparisonExpression>();
  EXPECT_EQ(cond_expr->GetChildrenSize(), 2);
  EXPECT_EQ(cond_expr->GetReturnValueType(), type::TypeId::BOOLEAN);
  EXPECT_EQ(cond_expr->GetExpressionType(), parser::ExpressionType::COMPARE_EQUAL);
  EXPECT_EQ(cond_expr->GetChild(1)->GetExpressionType(), parser::ExpressionType::VALUE_CONSTANT);
  auto const_expr = cond_expr->GetChild(1).CastManagedPointerTo<parser::ConstantValueExpression>();
  EXPECT_EQ(const_expr->GetReturnValueType(), type::TypeId::INTEGER);
  auto col_expr = cond_expr->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col_expr->GetReturnValueType(), type::TypeId::INTEGER);
  EXPECT_EQ(col_expr->GetTableOid(), table_a_oid_);
  EXPECT_EQ(col_expr->GetColumnOid(), catalog::col_oid_t(1));
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, SimpleFunctionCallTest) {
  std::string query = "SELECT cot(1.0) FROM a;";

  auto parse_tree = parser::PostgresParser::BuildParseTree(query);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);

  auto select_stmt = parse_tree->GetStatement(0).CastManagedPointerTo<parser::SelectStatement>();

  auto fun_expr = select_stmt->GetSelectColumns()[0].CastManagedPointerTo<parser::FunctionExpression>();
  auto proc_oid = fun_expr->GetProcOid();
  EXPECT_EQ(proc_oid, catalog::postgres::COT_PRO_OID);

  // Make a query with wrong argument types to check correct overloading
  query = "SELECT cot(1.0, 2.0) FROM a;";

  parse_tree = parser::PostgresParser::BuildParseTree(query);
  statement = parse_tree->GetStatements()[0];
  EXPECT_THROW(binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr), BinderException);
}

}  // namespace noisepage
