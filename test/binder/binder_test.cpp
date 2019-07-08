#include <memory>
#include "transaction/transaction_manager.h"
#include "util/test_harness.h"
#include "binder/bind_node_visitor.h"
#include "catalog/catalog.h"
#include "traffic_cop/statement.h"
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
using std::string;
using std::unique_ptr;
using std::vector;

namespace terrier {

// TODO (Ling): write meaningful setup
class BinderCorrectnessTest : public TerrierTest {
  virtual void SetUp() override {
    TerrierTest::SetUp();
    catalog::Catalog::GetInstance();
    // NOTE: Catalog::GetInstance()->Bootstrap(), you can only call it once!
    TestingExecutorUtil::InitializeDatabase(DEFAULT_DB_NAME);
  }

  virtual void TearDown() override {
    TestingExecutorUtil::DeleteDatabase(DEFAULT_DB_NAME);
    PelotonTest::TearDown();
  }
};

void SetupTables(std::string database_name) {
  LOG_INFO("Creating database %s", database_name.c_str());

  // create a transaction
  auto &txn_manager = transaction::TransactionManagerFactory::GetInstance();
  // begin transaction
  auto txn = txn_manager.BeginTransaction();
  // GetInstance will get a global catalog
  // then a database is created with the transaction as context and the database name
  // correspond to CatalogAccessor.createDatabase(database_name), as the accessor has the transactio as an attribute
  catalog::Catalog::GetInstance()->CreateDatabase(txn, database_name);
  // commit the transactions
  txn_manager.Commit(txn);
  LOG_INFO("database %s created!", database_name.c_str());

  // get a parser instance
  auto &parser = parser::PostgresParser::GetInstance();
  // get a traffic cop instance
  auto &traffic_cop = tcop::TrafficCop::GetInstance();
  // set the default database name to the name of the database we just created
  traffic_cop.SetDefaultDatabaseName(database_name);
  // set the task call back of the traffic cop
  // counter
  traffic_cop.SetTaskCallback(TestingSQLUtil::UtilTestTaskCallback,
                              &TestingSQLUtil::counter_);

  // get an optimizer instance
  optimizer::Optimizer optimizer;

  // sql statements for creating test tables
  vector<string> createTableSQLs{"CREATE TABLE A(A1 int, a2 varchar)",
                                 "CREATE TABLE b(B1 int, b2 varchar)"};
  // for each statement
  for (auto &sql : createTableSQLs) {
    LOG_INFO("%s", sql.c_str());
    // begin transaction
    txn = txn_manager.BeginTransaction();
    // set transaction state to success?
    traffic_cop.SetTcopTxnState(txn);

    vector<type::Value> params;
    vector<ResultValue> result;
    vector<int> result_format;
    // create a CREATE statement instance
    unique_ptr<Statement> statement(new Statement("CREATE", sql));

    // build the parse tree for the sql statement
    // return std::unique_ptr<parser::SQLStatementList>
    auto parse_tree_list = parser.BuildParseTree(sql);
    // get the first statement
    auto parse_tree = parse_tree_list->GetStatement(0);
    // get a instance of bindNodeBVisitor by specifying the transaction and the database name
    // also the bindNodeVisitor fills its catalog attribute with the global catalog
    // and the binder_context with nullptr
    auto bind_node_visitor = binder::BindNodeVisitor(txn, database_name);
    // the bindNodeVisitor travers the parse tree and bind the name to node
    // essentially it is using the Accept() method of the statement object
    // which calls visit on the sqlNodeVisitor
    // which recursively visit the children
    bind_node_visitor.BindNameToNode(parse_tree);

    // use the parse tree and transaction to build the plan tree
    statement->SetPlanTree(
        optimizer.BuildPelotonPlanTree(parse_tree_list, txn));
    TestingSQLUtil::counter_.store(1);
    // execute the statement with plan
    auto status = traffic_cop.ExecuteHelper(statement->GetPlanTree(), params, result, result_format);

    if (traffic_cop.GetQueuing()) {
      TestingSQLUtil::ContinueAfterComplete();
      traffic_cop.ExecuteStatementPlanGetResult();
      status = traffic_cop.p_status_;
      traffic_cop.SetQueuing(false);
    }
    LOG_INFO("Table create result: %s",
             ResultTypeToString(status.m_result).c_str());

    // commit
    traffic_cop.CommitQueryHelper();
  }
}

// NOLINTNEXTLINE
TEST_F(BinderCorrectnessTest, SelectStatementTest) {
  std::string default_database_name = "test_db";
  SetupTables(default_database_name);
  parser::PostgresParser parser;

//  catalog::Catalog *catalog_ptr = catalog::Catalog::GetInstance();
//  catalog_ptr->Bootstrap();

  // Test regular table name
  LOG_INFO("Parsing sql query");

//  auto &txn_manager = transaction::TransactionManagerFactory::GetInstance();
  storage::RecordBufferSegmentPool buffer_pool = {100, 100};
  auto txn_manager = new transaction::TransactionManager{&buffer_pool, true, LOGGING_DISABLED};
  auto txn = txn_manager->BeginTransaction();

  // initialize a block store
  auto *block_store = new storage::BlockStore(100, 100);
  // new catalog requires txn_manage and block_store as parameters
  auto catalog = new catalog::Catalog(txn_manager, block_store);
  auto accessor = catalog->GetAccessor(txn, catalog->GetDatabaseOid(txn, default_database_name));

  unique_ptr<binder::BindNodeVisitor> binder(new binder::BindNodeVisitor(accessor, default_database_name));
//  auto binder = new binder::BindNodeVisitor(accessor, default_database_name);

  string selectSQL = "SELECT A.a1, B.b2 FROM A INNER JOIN b ON a.a1 = b.b1 WHERE a1 < 100 "
                     "GROUP BY A.a1, B.b2 HAVING a1 > 50 ORDER BY a1";

  auto parse_tree = parser.BuildParseTree(selectSQL);
  auto selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree[0].get());
  binder->BindNameToNode(selectStmt);

  auto db_oid = accessor->GetDatabaseOid(default_database_name);
  auto tableA_oid = accessor->GetTableOid("a");
  auto tableB_oid = accessor->GetTableOid("b");
  txn_manager->Commit(txn);

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

  txn = txn_manager.BeginTransaction();
  binder.reset(new binder::BindNodeVisitor(accessor, default_database_name));
  selectSQL = "SELECT * FROM A, B as A";
  parse_tree = parser.BuildParseTree(selectSQL);
  selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree[0].get());
  try {
    binder->BindNameToNode(selectStmt);
    EXPECT_TRUE(false);
  } catch (Exception &e) {
    LOG_INFO("Correct! Exception(%s) catched", e.what());
  }

  // Test select from different table instances from the same physical schema
  txn_manager->Commit(txn);

  txn = txn_manager->BeginTransaction();
  binder.reset(new binder::BindNodeVisitor(accessor, default_database_name));
  selectSQL = "SELECT * FROM A, A as AA where A.a1 = AA.a2";
  parse_tree = parser.BuildParseTree(selectSQL);
  selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree[0].get());
  binder->BindNameToNode(selectStmt);
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
  txn_manager->Commit(txn);

  txn = txn_manager->BeginTransaction();
  binder.reset(new binder::BindNodeVisitor(accessor, default_database_name));
  selectSQL = "SELECT AA.a1, b2 FROM A as AA, B WHERE AA.a1 = B.b1";
  parse_tree = parser.BuildParseTree(selectSQL);
  selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree[0].get());
  binder->BindNameToNode(selectStmt);
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
  
  txn_manager->Commit(txn);
  // Delete the test database
  txn = txn_manager->BeginTransaction();
  accessor->DropDatabase(db_oid);
  txn_manager.Commit(txn);
}

// TODO: add test for Update Statement. Currently UpdateStatement uses char*
// instead of ColumnValueExpression to represent column. We can only add this
// test after UpdateStatement is changed

TEST_F(BinderCorrectnessTest, DeleteStatementTest) {
std::string default_database_name = "test_db";
SetupTables(default_database_name);
auto &parser = parser::PostgresParser::GetInstance();
catalog::Catalog *catalog_ptr = catalog::Catalog::GetInstance();

auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
auto txn = txn_manager.BeginTransaction();
oid_t db_oid =
    catalog_ptr->GetDatabaseWithName(txn, default_database_name)->GetOid();
oid_t tableB_oid = catalog_ptr
    ->GetTableWithName(txn, default_database_name, DEFAULT_SCHEMA_NAME, "b")
    ->GetOid();

string deleteSQL = "DELETE FROM b WHERE 1 = b1 AND b2 = 'str'";
unique_ptr<binder::BindNodeVisitor> binder(
    new binder::BindNodeVisitor(txn, default_database_name));

auto parse_tree = parser.BuildParseTree(deleteSQL);
auto deleteStmt = dynamic_cast<parser::DeleteStatement *>(
    parse_tree->GetStatements().at(0).get());
binder->BindNameToNode(deleteStmt);

txn_manager.Commit(txn);

LOG_INFO("Checking first condition in where clause");
auto col_expr = dynamic_cast<const parser::ColumnValueExpression *>(
    deleteStmt->expr->GetChild(0)->GetChild(1));
EXPECT_EQ(col_expr->GetBoundOid(), make_tuple(db_oid, tableB_oid, 0));

LOG_INFO("Checking second condition in where clause");
col_expr = dynamic_cast<const parser::ColumnValueExpression *>(
    deleteStmt->expr->GetChild(1)->GetChild(0));
EXPECT_EQ(col_expr->GetBoundOid(), make_tuple(db_oid, tableB_oid, 1));

// Delete the test database
txn = txn_manager.BeginTransaction();
catalog_ptr->DropDatabaseWithName(txn, default_database_name);
txn_manager.Commit(txn);
}

TEST_F(BinderCorrectnessTest, BindDepthTest) {
std::string default_database_name = "test_db";
SetupTables(default_database_name);
auto &parser = parser::PostgresParser::GetInstance();

// Test regular table name
LOG_INFO("Parsing sql query");

auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
auto txn = txn_manager.BeginTransaction();
unique_ptr<binder::BindNodeVisitor> binder(
    new binder::BindNodeVisitor(txn, default_database_name));
string selectSQL =
    "SELECT A.a1 FROM A WHERE A.a1 IN (SELECT b1 FROM B WHERE b1 = 2 AND b2 "
    "> (SELECT a1 FROM A WHERE a2 > 0)) "
    "AND EXISTS (SELECT b1 FROM B WHERE B.b1 = A.a1)";

auto parse_tree = parser.BuildParseTree(selectSQL);
auto selectStmt = dynamic_cast<parser::SelectStatement *>(
    parse_tree->GetStatements().at(0).get());
binder->BindNameToNode(selectStmt);
txn_manager.Commit(txn);

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
txn = txn_manager.BeginTransaction();
catalog_ptr->DropDatabaseWithName(txn, default_database_name);
txn_manager.Commit(txn);
}

TEST_F(BinderCorrectnessTest, FunctionExpressionTest) {
auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
auto txn = txn_manager.BeginTransaction();

string function_sql = "SELECT substr('test123', a, 3)";
auto &parser = parser::PostgresParser::GetInstance();
auto parse_tree = parser.BuildParseTree(function_sql);
auto stmt = parse_tree->GetStatement(0);
unique_ptr<binder::BindNodeVisitor> binder(
    new binder::BindNodeVisitor(txn, DEFAULT_DB_NAME));
EXPECT_THROW(binder->BindNameToNode(stmt), peloton::Exception);

function_sql = "SELECT substr('test123', 2, 3)";
auto parse_tree2 = parser.BuildParseTree(function_sql);
stmt = parse_tree2->GetStatement(0);
binder->BindNameToNode(stmt);
auto funct_expr = dynamic_cast<parser::FunctionExpression *>(
    dynamic_cast<parser::SelectStatement *>(stmt)->select_list[0].get());
EXPECT_TRUE(funct_expr->Evaluate(nullptr, nullptr, nullptr)
.CompareEquals(type::ValueFactory::GetVarcharValue("est")) ==
CmpBool::CmpTrue);

txn_manager.Commit(txn);
}

}  // namespace peloton
