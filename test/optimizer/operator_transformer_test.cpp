#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "binder/bind_node_visitor.h"
#include "catalog/catalog.h"
#include "optimizer/operator_expression.h"
#include "optimizer/query_to_operator_transformer.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/operator_expression.h"
#include "parser/expression/subquery_expression.h"
#include "parser/postgresparser.h"
#include "storage/garbage_collector.h"
#include "traffic_cop/statement.h"
#include "traffic_cop/traffic_cop.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_manager.h"
#include "util/test_harness.h"
#include "util/transaction_benchmark_util.h"

using std::make_shared;
using std::make_tuple;

using std::unique_ptr;
using std::vector;

namespace terrier {

class OperatorTransformerTest : public TerrierTest {
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
  transaction::TimestampManager *timestamp_manager_;
  transaction::DeferredActionManager *deferred_action_manager_;
  transaction::TransactionManager *txn_manager_;
  transaction::TransactionContext *txn_;
  std::unique_ptr<catalog::CatalogAccessor> accessor_;
  binder::BindNodeVisitor *binder_;
  optimizer::QueryToOperatorTransformer *operator_transformer_;
  std::vector<optimizer::OpType> op_types_;

  void SetUpTables() {
    // Initialize the transaction manager and GC
    timestamp_manager_ = new transaction::TimestampManager;
    deferred_action_manager_ = new transaction::DeferredActionManager(timestamp_manager_);
    txn_manager_ = new transaction::TransactionManager(timestamp_manager_, deferred_action_manager_, &buffer_pool_,
                                                       true, DISABLED);
    gc_ = new storage::GarbageCollector(timestamp_manager_, deferred_action_manager_, txn_manager_, nullptr);

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
    delete deferred_action_manager_;
    delete timestamp_manager_;
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

  const std::string GetInfo(optimizer::OperatorExpression *op) const {
    std::string info = "{";
    {
      info += "\"Op\":";
      info += "\"" + op->GetOp().GetName() + "\",";
      auto children = op->GetChildren();
      if (!children.empty()) {
        info += "\"Children\":[";
        {
          bool is_first = true;
          for (const auto &child : children) {
            if (is_first == true) {
              is_first = false;
            } else {
              info += ",";
            }
            info += GetInfo(child);
          }
        }
        info += "]";
      }
    }
    info += '}';
    return info;
  }
};

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SelectStatementSimpleTest) {
  // Test regular table name
  LOG_INFO("Parsing sql query");
  std::string selectSQL = "SELECT A.A1 FROM A";

  std::string ref =
      "{\"Op\":\"LogicalGet\",}";

  auto parse_tree = parser_.BuildParseTree(selectSQL);
  auto selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree.GetStatements()[0].get());
  binder_->BindNameToNode(selectStmt);
  auto accessor_ = binder_->GetCatalogAccessor();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  auto operator_tree = operator_transformer_->ConvertToOpExpression(selectStmt);
  auto info = GetInfo(operator_tree);

  EXPECT_EQ(ref, info);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SelectStatementAggregateTest) {
  // Test regular table name
  LOG_INFO("Parsing sql query");
  std::string selectSQL = "SELECT MAX(b1) FROM B";

  std::string ref =
      "{\"Op\":\"LogicalAggregateAndGroupBy\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",}]}";

  auto parse_tree = parser_.BuildParseTree(selectSQL);
  auto selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree.GetStatements()[0].get());
  binder_->BindNameToNode(selectStmt);
  auto accessor_ = binder_->GetCatalogAccessor();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  auto operator_tree = operator_transformer_->ConvertToOpExpression(selectStmt);
  auto info = GetInfo(operator_tree);

  EXPECT_EQ(ref, info);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SelectStatementComplexTest) {
  // Test regular table name
  LOG_INFO("Parsing sql query");
  std::string selectSQL =
      "SELECT A.A1, B.B2 FROM A INNER JOIN b ON a.a1 = b.b1 WHERE a1 < 100 "
      "GROUP BY A.a1, B.b2 HAVING a1 > 50 ORDER BY a1";

  std::string ref =
      "{\"Op\":\"LogicalFilter\",\"Children\":"
      "[{\"Op\":\"LogicalAggregateAndGroupBy\",\"Children\":"
      "[{\"Op\":\"LogicalFilter\",\"Children\":"
      "[{\"Op\":\"LogicalInnerJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalGet\",}]}]}]}]}";

  auto parse_tree = parser_.BuildParseTree(selectSQL);
  auto selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree.GetStatements()[0].get());
  binder_->BindNameToNode(selectStmt);
  auto accessor_ = binder_->GetCatalogAccessor();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  auto operator_tree = operator_transformer_->ConvertToOpExpression(selectStmt);
  auto info = GetInfo(operator_tree);

  EXPECT_EQ(ref, info);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SelectStatementStarTest) {
  // Check if star expression is correctly processed
  LOG_INFO("Checking STAR expression in select and subselect");

  std::string selectSQL = "SELECT * FROM A LEFT OUTER JOIN B ON A.A1 < B.B1";

  std::string ref =
      "{\"Op\":\"LogicalLeftJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalGet\",}]}";

  auto parse_tree = parser_.BuildParseTree(selectSQL);
  auto selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree.GetStatements()[0].get());
  binder_->BindNameToNode(selectStmt);
  auto accessor_ = binder_->GetCatalogAccessor();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  auto operator_tree = operator_transformer_->ConvertToOpExpression(selectStmt);
  auto info = GetInfo(operator_tree);

  EXPECT_EQ(ref, info);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SelectStatementStarNestedSelectTest) {
  // Check if star expression is correctly processed
  LOG_INFO("Checking STAR expression in nested select from.");

  std::string selectSQL =
      "SELECT * FROM A LEFT OUTER JOIN (SELECT * FROM B INNER JOIN A ON B1 = A1) AS C ON C.B2 = a.A1";

  std::string ref =
      "{\"Op\":\"LogicalLeftJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalQueryDerivedGet\",\"Children\":"
      "[{\"Op\":\"LogicalFilter\",\"Children\":"
      "[{\"Op\":\"LogicalInnerJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalGet\",}]}]}]}]}";

  auto parse_tree = parser_.BuildParseTree(selectSQL);
  auto selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree.GetStatements()[0].get());
  binder_->BindNameToNode(selectStmt);
  auto accessor_ = binder_->GetCatalogAccessor();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  auto operator_tree = operator_transformer_->ConvertToOpExpression(selectStmt);
  auto info = GetInfo(operator_tree);

  EXPECT_EQ(ref, info);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SelectStatementNestedColumnTest) {
  // Check if nested select columns are correctly processed
  LOG_INFO("Checking nested select columns.");

  std::string selectSQL = "SELECT A1, (SELECT B2 FROM B where B2 IS NULL LIMIT 1) FROM A";

  std::string ref =
      "{\"Op\":\"LogicalGet\",}";

  auto parse_tree = parser_.BuildParseTree(selectSQL);
  auto selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree.GetStatements()[0].get());
  binder_->BindNameToNode(selectStmt);
  auto accessor_ = binder_->GetCatalogAccessor();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  auto operator_tree = operator_transformer_->ConvertToOpExpression(selectStmt);
  auto info = GetInfo(operator_tree);

  EXPECT_EQ(ref, info);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SelectStatementDiffTableSameSchemaTest) {
  // Test select from different table instances from the same physical schema
  std::string selectSQL = "SELECT * FROM A, A as AA where A.a1 = AA.a2";

  std::string ref =
      "{\"Op\":\"LogicalFilter\",\"Children\":"
      "[{\"Op\":\"LogicalInnerJoin\",\"Children\":"
      "[{\"Op\":\"LogicalInnerJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalGet\",}]},{\"Op\":\"LogicalGet\",}]}]}";

  auto parse_tree = parser_.BuildParseTree(selectSQL);
  auto selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree.GetStatements()[0].get());
  binder_->BindNameToNode(selectStmt);
  auto accessor_ = binder_->GetCatalogAccessor();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  auto operator_tree = operator_transformer_->ConvertToOpExpression(selectStmt);
  auto info = GetInfo(operator_tree);

  EXPECT_EQ(ref, info);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SelectStatementSelectListAliasTest) {
  // Test alias and select_list
  LOG_INFO("Checking select_list and table alias binding");

  std::string selectSQL = "SELECT AA.a1, b2 FROM A as AA, B WHERE AA.a1 = B.b1";

  std::string ref =
      "{\"Op\":\"LogicalFilter\",\"Children\":"
      "[{\"Op\":\"LogicalInnerJoin\",\"Children\":"
      "[{\"Op\":\"LogicalInnerJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalGet\",}]},{\"Op\":\"LogicalGet\",}]}]}";

  auto parse_tree = parser_.BuildParseTree(selectSQL);
  auto selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree.GetStatements()[0].get());
  binder_->BindNameToNode(selectStmt);
  auto accessor_ = binder_->GetCatalogAccessor();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  auto operator_tree = operator_transformer_->ConvertToOpExpression(selectStmt);
  auto info = GetInfo(operator_tree);

  EXPECT_EQ(ref, info);
}

// TODO(peloton): add test for Update Statement. Currently UpdateStatement uses char*
// instead of ColumnValueExpression to represent column. We can only add this
// test after UpdateStatement is changed

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, DeleteStatementWhereTest) {
  std::string deleteSQL = "DELETE FROM b WHERE 1 = b1 AND b2 = 'str'";

  std::string ref =
      "{\"Op\":\"LogicalDelete\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",}]}";
  auto parse_tree = parser_.BuildParseTree(deleteSQL);
  auto deleteStmt = dynamic_cast<parser::DeleteStatement *>(parse_tree.GetStatements()[0].get());
  binder_->BindNameToNode(deleteStmt);
  auto accessor_ = binder_->GetCatalogAccessor();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  auto operator_tree = operator_transformer_->ConvertToOpExpression(deleteStmt);
  auto info = GetInfo(operator_tree);

  EXPECT_EQ(ref, info);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, AggregateSimpleTest) {
  // Check if nested select columns are correctly processed
  LOG_INFO("Checking simple aggregate select.");

  std::string selectSQL = "SELECT MAX(b1) FROM B;";

  std::string ref =
      "{\"Op\":\"LogicalAggregateAndGroupBy\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",}]}";

  auto parse_tree = parser_.BuildParseTree(selectSQL);
  auto selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree.GetStatements()[0].get());
  binder_->BindNameToNode(selectStmt);
  auto accessor_ = binder_->GetCatalogAccessor();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  auto operator_tree = operator_transformer_->ConvertToOpExpression(selectStmt);
  auto info = GetInfo(operator_tree);

  EXPECT_EQ(ref, info);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, AggregateComplexTest) {
  // Check if nested select columns are correctly processed
  LOG_INFO("Checking aggregate in subselect.");

  std::string selectSQL = "SELECT A.a1 FROM A WHERE A.a1 IN (SELECT MAX(b1) FROM B);";

  std::string ref =
      "{\"Op\":\"LogicalLimit\",\"Children\":"
      "[{\"Op\":\"LogicalLeftJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalGet\",}]}]}";

  auto parse_tree = parser_.BuildParseTree(selectSQL);
  auto selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree.GetStatements()[0].get());
  binder_->BindNameToNode(selectStmt);
  auto accessor_ = binder_->GetCatalogAccessor();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  auto operator_tree = operator_transformer_->ConvertToOpExpression(selectStmt);
  auto info = GetInfo(operator_tree);

  EXPECT_EQ(ref, info);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, OperatorComplexTest) {
  // Check if nested select columns are correctly processed
  LOG_INFO("Checking if operator expressions are correctly parsed.");

  std::string selectSQL = "SELECT A.a1 FROM A WHERE 2 * A.a1 IN (SELECT b1+1 FROM B);";

  std::string ref =
      "{\"Op\":\"LogicalLimit\",\"Children\":"
      "[{\"Op\":\"LogicalLeftJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalGet\",}]}]}";

  auto parse_tree = parser_.BuildParseTree(selectSQL);
  auto selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree.GetStatements()[0].get());
  binder_->BindNameToNode(selectStmt);
  auto accessor_ = binder_->GetCatalogAccessor();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  auto operator_tree = operator_transformer_->ConvertToOpExpression(selectStmt);
  auto info = GetInfo(operator_tree);

  EXPECT_EQ(ref, info);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SubqueryComplexTest) {
  // Test regular table name
  LOG_INFO("Parsing sql query");

  std::string selectSQL =
      "SELECT A.a1 FROM A WHERE A.a1 IN (SELECT b1 FROM B WHERE b1 = 2 AND "
      "b2 > (SELECT a1 FROM A WHERE a2 > 0)) AND EXISTS (SELECT b1 FROM B WHERE B.b1 = A.a1)";

  std::string ref =
      "{\"Op\":\"LogicalLimit\",\"Children\":"
      "[{\"Op\":\"LogicalLeftJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalGet\",}]}]}";

  auto parse_tree = parser_.BuildParseTree(selectSQL);
  auto selectStmt = dynamic_cast<parser::SelectStatement *>(parse_tree.GetStatements()[0].get());
  binder_->BindNameToNode(selectStmt);
  auto accessor_ = binder_->GetCatalogAccessor();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  auto operator_tree = operator_transformer_->ConvertToOpExpression(selectStmt);
  auto info = GetInfo(operator_tree);

  EXPECT_EQ(ref, info);
}
}  // namespace terrier
