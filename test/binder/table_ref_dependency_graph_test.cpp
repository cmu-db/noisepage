#include "binder/table_ref_dependency_graph.h"

#include <memory>

#include "binder/bind_node_visitor.h"
#include "catalog/catalog.h"
#include "common/managed_pointer.h"
#include "main/db_main.h"
#include "parser/postgresparser.h"
#include "parser/statements.h"
#include "test_util/test_harness.h"
#include "transaction/transaction_manager.h"

namespace noisepage {
class TableRefDependencyGraphTest : public TerrierTest {
 public:
  void InitializeTables() {
    // Create database
    txn_ = txn_manager_->BeginTransaction();
    db_oid_ = catalog_->CreateDatabase(common::ManagedPointer{txn_}, database_name_, true);

    // Commit the transaction
    txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Get default values of the columns
    auto int_default = parser::ConstantValueExpression(type::TypeId::INTEGER);

    // Create table
    txn_ = txn_manager_->BeginTransaction();
    accessor_ = catalog_->GetAccessor(common::ManagedPointer(txn_), db_oid_, DISABLED);

    // CREATE TABLE TEST(x INT, y INT)
    std::vector<catalog::Schema::Column> cols{};
    cols.emplace_back("x", type::TypeId::INTEGER, true, int_default);
    cols.emplace_back("y", type::TypeId::INTEGER, true, int_default);
    catalog::Schema schema{cols};

    table_oid_ = accessor_->CreateTable(accessor_->GetDefaultNamespace(), table_name_, schema);
    auto table = new storage::SqlTable{db_main_->GetStorageLayer()->GetBlockStore(), schema};
    EXPECT_TRUE(accessor_->SetTablePointer(table_oid_, table));

    txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
    accessor_.reset(nullptr);
  }

  void SetUp() override {
    db_main_ = noisepage::DBMain::Builder().SetUseGC(true).SetUseCatalog(true).Build();
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
    catalog_ = db_main_->GetCatalogLayer()->GetCatalog();

    InitializeTables();

    // Prepare for testing
    txn_ = txn_manager_->BeginTransaction();
    accessor_ = catalog_->GetAccessor(common::ManagedPointer(txn_), db_oid_, DISABLED);
  }

  void TearDown() override {
    txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
    accessor_.reset(nullptr);
  }

 protected:
  // The test database name
  const std::string database_name_{"TestDB"};

  // The test table name
  const std::string table_name_{"TestTable"};

  // The DBMain instance
  std::unique_ptr<DBMain> db_main_;

  // The test database OID
  catalog::db_oid_t db_oid_;

  // The test table OID
  catalog::table_oid_t table_oid_;

  // The transaction manager
  transaction::TransactionContext *txn_;

  // The catalog instance
  common::ManagedPointer<catalog::Catalog> catalog_;

  // The catalog accessor
  std::unique_ptr<catalog::CatalogAccessor> accessor_;

  // The transaction manager
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
};

// ----------------------------------------------------------------------------
// Basic Tests for Graph Construction from Dependency Graph
// ----------------------------------------------------------------------------

// TODO(Kyle)

// ----------------------------------------------------------------------------
// Constructing Dependency Graph from Input Query
// ----------------------------------------------------------------------------

TEST_F(TableRefDependencyGraphTest, ExtractSelectStatementFromParseResult) {
  const std::string sql = "SELECT * FROM TestTable;";
  auto parse_tree = parser::PostgresParser::BuildParseTree(sql);

  EXPECT_EQ(parser::StatementType::SELECT, parse_tree->GetStatement(0)->GetType());

  auto select = parse_tree->GetStatement(0).CastManagedPointerTo<parser::SelectStatement>();

  EXPECT_EQ(select->GetDepth(), -1);

  EXPECT_TRUE(select->HasSelectTable());
  EXPECT_EQ(select->GetSelectTable()->GetAlias(), "testtable");
}

TEST_F(TableRefDependencyGraphTest, BuildGraphFromQuery0) {
  const std::string sql = "SELECT * FROM TestTable;";
  auto parse_tree = parser::PostgresParser::BuildParseTree(sql);

  EXPECT_EQ(parser::StatementType::SELECT, parse_tree->GetStatement(0)->GetType());
  auto select = parse_tree->GetStatement(0).CastManagedPointerTo<parser::SelectStatement>();

  // Construct a dependency graph from the query
  binder::TableRefDependencyGraph graph{select->GetSelectWith(), common::ManagedPointer{accessor_}};

  // Construct the expected graph
  binder::AliasAdjacencyList expected{};

  // Check
  EXPECT_TRUE(binder::AliasAdjacencyList::Equals(graph.AdjacencyList(), expected));
}

TEST_F(TableRefDependencyGraphTest, BuildGraphFromQuery1) {
  const std::string sql = "WITH x(i) AS (SELECT 1) SELECT * FROM x;";
  auto parse_tree = parser::PostgresParser::BuildParseTree(sql);

  EXPECT_EQ(parser::StatementType::SELECT, parse_tree->GetStatement(0)->GetType());
  auto select = parse_tree->GetStatement(0).CastManagedPointerTo<parser::SelectStatement>();

  // Construct a dependency graph from the query
  binder::TableRefDependencyGraph graph{select->GetSelectWith(), common::ManagedPointer{accessor_}};

  // Construct the expected graph
  binder::AliasAdjacencyList expected{};

  // Check
  EXPECT_TRUE(binder::AliasAdjacencyList::Equals(graph.AdjacencyList(), expected));
}

TEST_F(TableRefDependencyGraphTest, BuildGraphFromQuery2) {
  const std::string sql = "WITH x(i) AS (SELECT 1), y(j) AS (SELECT * FROM x) SELECT * FROM y;";
  auto parse_tree = parser::PostgresParser::BuildParseTree(sql);

  EXPECT_EQ(parser::StatementType::SELECT, parse_tree->GetStatement(0)->GetType());
  auto select = parse_tree->GetStatement(0).CastManagedPointerTo<parser::SelectStatement>();

  // Construct a dependency graph from the query
  binder::TableRefDependencyGraph graph{select->GetSelectWith(), common::ManagedPointer{accessor_}};

  // Construct the expected graph
  binder::AliasAdjacencyList expected{};
  expected.AddEdge("y", "x");

  // Check
  EXPECT_TRUE(binder::AliasAdjacencyList::Equals(graph.AdjacencyList(), expected));
}

}  // namespace noisepage
