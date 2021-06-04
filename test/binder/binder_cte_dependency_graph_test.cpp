#include <memory>

#include "binder/bind_node_visitor.h"
#include "binder/cte/dependency_graph.h"
#include "catalog/catalog.h"
#include "common/managed_pointer.h"
#include "main/db_main.h"
#include "parser/postgresparser.h"
#include "parser/statements.h"
#include "test_util/test_harness.h"
#include "transaction/transaction_manager.h"

using noisepage::binder::cte::DependencyGraph;

namespace noisepage {
class BinderCteDepdendencyGraphTest : public TerrierTest {
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

/**
 * Get the SELECT statement from a raw SQL query.
 * @param sql The query string
 * @return A pair of the parse tree and the SELECT statement;
 * we must return both in order to extend the parse tree's lifetime
 */
static std::pair<std::unique_ptr<parser::ParseResult>, common::ManagedPointer<parser::SelectStatement>>
ParseToSelectStatement(const std::string &sql) {
  auto parse_tree = parser::PostgresParser::BuildParseTree(sql);
  if (parse_tree->GetStatement(0)->GetType() != parser::StatementType::SELECT) {
    // Just die, don't really care how
    throw std::runtime_error{""};
  }
  auto select = parse_tree->GetStatement(0).CastManagedPointerTo<parser::SelectStatement>();
  return std::make_pair(std::move(parse_tree), select);
}

// /**
//  * Build an edge from `src` to `dst`.
//  * @param src The source vertex, as std::tuple
//  * @param dst The destination vertex, as std::tuple
//  * @return The new edge
//  */
// static TableDependencyGraph::Edge MakeEdge(const std::tuple<std::string, std::size_t, std::size_t> &src,
//                                            const std::tuple<std::string, std::size_t, std::size_t> &dst) {
//   return TableDependencyGraph::Edge{TableDependencyGraph::Vertex{std::get<0>(src), std::get<1>(src),
//   std::get<2>(src)},
//                                     TableDependencyGraph::Vertex{std::get<0>(dst), std::get<1>(dst),
//                                     std::get<2>(dst)}};
// }

TEST_F(BinderCteDepdendencyGraphTest, ItWorks) {
  const std::string sql = "SELECT * FROM TestTable;";
  auto [_, select] = ParseToSelectStatement(sql);
  EXPECT_EQ(select->GetDepth(), -1);
}

// ----------------------------------------------------------------------------
// Dependency Graph Validation: Forward References
// ----------------------------------------------------------------------------

// ----------------------------------------------------------------------------
// Dependency Graph Validation: Mutual Recursion
// ----------------------------------------------------------------------------

// ----------------------------------------------------------------------------
// Dependency Graph Validation: Nested Scope
// ----------------------------------------------------------------------------

// TEST_F(BinderCteUtilTest, CheckNestedScope0) {
//   const std::string sql =
//       "WITH x(i) AS (WITH a(m) AS (SELECT 1) SELECT * FROM a), y(j) AS (SELECT * FROM x) SELECT * FROM y;";
//   auto [_, select] = ParseToSelectStatement(sql);

//   TableDependencyGraph graph{select};
//   EXPECT_TRUE(graph.CheckNestedScopes());
// }

// TEST_F(BinderCteUtilTest, CheckNestedScope1) {
//   const std::string sql =
//       "WITH x(i) AS (WITH a(m) AS (SELECT 1) SELECT * FROM a), y(j) AS (SELECT * FROM a) SELECT * FROM y;";
//   auto [_, select] = ParseToSelectStatement(sql);

//   TableDependencyGraph graph{select};
//   EXPECT_FALSE(graph.CheckNestedScopes());
// }

}  // namespace noisepage
