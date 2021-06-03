#include "binder/cte/structured_statement.h"

#include <memory>

#include "binder/bind_node_visitor.h"
#include "binder/cte/context_sensitive_table_ref.h"
#include "catalog/catalog.h"
#include "common/managed_pointer.h"
#include "main/db_main.h"
#include "parser/postgresparser.h"
#include "parser/statements.h"
#include "test_util/test_harness.h"
#include "transaction/transaction_manager.h"

using noisepage::binder::cte::StructuredStatement;

namespace noisepage {
class BinderCteStructuredStatementTest : public TerrierTest {
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

/**
 * Check that internal identifiers follow the expected pattern in `graph`.
 * @param graph The graph of interest
 * @return `true` if the check passes, `false` otherwise
 */
static bool CheckIdentifiers(const StructuredStatement &statement) {
  std::vector<std::size_t> expected(statement.RefCount());
  std::iota(expected.begin(), expected.end(), 0UL);
  const auto identifiers = statement.Identifiers();
  std::vector<std::size_t> actual{identifiers.cbegin(), identifiers.cend()};
  std::sort(actual.begin(), actual.end());
  return std::equal(actual.cbegin(), actual.cend(), expected.cbegin());
}

TEST_F(BinderCteStructuredStatementTest, ItWorks) {
  const std::string sql = "SELECT * FROM TestTable;";
  auto [_, select] = ParseToSelectStatement(sql);
  EXPECT_EQ(select->GetDepth(), -1);
  EXPECT_TRUE(select->HasSelectTable());
  EXPECT_EQ(select->GetSelectTable()->GetAlias(), "testtable");
}

TEST_F(BinderCteStructuredStatementTest, BuildStatement0) {
  const std::string sql = "SELECT * FROM TestTable;";
  auto [_, select] = ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  EXPECT_EQ(1UL, statement.RefCount());
  EXPECT_EQ(0UL, statement.DependencyCount());
  EXPECT_EQ(1UL, statement.ScopeCount());
  EXPECT_TRUE(statement.HasReadRef({"testtable", 0UL}));
  EXPECT_TRUE(CheckIdentifiers(statement));
}

// TEST_F(BinderCteUtilTest, BuildTableDependencyGraph1) {
//   const std::string sql = "WITH x(i) AS (SELECT 1) SELECT * FROM x;";
//   auto [_, select] = ParseToSelectStatement(sql);

//   TableDependencyGraph graph{select};
//   EXPECT_EQ(1UL, graph.Order());
//   EXPECT_EQ(0UL, graph.Size());
//   EXPECT_TRUE(graph.HasVertex({"x", 0UL, 0UL}));
//   EXPECT_TRUE(CheckIdentifiers(graph));
// }

// TEST_F(BinderCteUtilTest, BuildTableDependencyGraph2) {
//   const std::string sql = "WITH x(i) AS (SELECT 1), y(j) AS (SELECT 1) SELECT * FROM y;";
//   auto [_, select] = ParseToSelectStatement(sql);

//   TableDependencyGraph graph{select};
//   EXPECT_EQ(2UL, graph.Order());
//   EXPECT_EQ(0UL, graph.Size());
//   EXPECT_TRUE(graph.HasVertex({"x", 0UL, 0UL}));
//   EXPECT_TRUE(graph.HasVertex({"y", 0UL, 1UL}));
//   EXPECT_TRUE(CheckIdentifiers(graph));
// }

// TEST_F(BinderCteUtilTest, BuildTableDependencyGraph3) {
//   const std::string sql = "WITH x(i) AS (SELECT 1), y(j) AS (SELECT 1), z(k) AS (SELECT 1) SELECT * FROM z;";
//   auto [_, select] = ParseToSelectStatement(sql);

//   TableDependencyGraph graph{select};
//   EXPECT_EQ(3UL, graph.Order());
//   EXPECT_EQ(0UL, graph.Size());
//   EXPECT_TRUE(graph.HasVertex({"x", 0UL, 0UL}));
//   EXPECT_TRUE(graph.HasVertex({"y", 0UL, 1UL}));
//   EXPECT_TRUE(graph.HasVertex({"z", 0UL, 2UL}));
//   EXPECT_TRUE(CheckIdentifiers(graph));
// }

// TEST_F(BinderCteUtilTest, BuildTableDependencyGraph4) {
//   // Backward reference y -> x
//   const std::string sql = "WITH x(i) AS (SELECT 1), y(j) AS (SELECT * FROM x) SELECT * FROM y;";
//   auto [_, select] = ParseToSelectStatement(sql);

//   TableDependencyGraph graph{select};
//   EXPECT_EQ(3UL, graph.Order());
//   EXPECT_EQ(1UL, graph.Size());
//   EXPECT_TRUE(graph.HasVertex({"x", 0UL, 0UL}));
//   EXPECT_TRUE(graph.HasVertex({"y", 0UL, 1UL}));
//   EXPECT_TRUE(graph.HasVertex({"x", 0UL, 1UL}));
//   EXPECT_TRUE(graph.HasEdge(MakeEdge({"y", 0UL, 1UL}, {"x", 0UL, 1UL})));
//   EXPECT_TRUE(CheckIdentifiers(graph));
// }

// TEST_F(BinderCteUtilTest, BuildTableDependencyGraph5) {
//   // Backward reference y -> x
//   const std::string sql = "WITH x(i) AS (SELECT 1), y(j) AS (SELECT i FROM x) SELECT * FROM y;";
//   auto [_, select] = ParseToSelectStatement(sql);

//   TableDependencyGraph graph{select};
//   EXPECT_EQ(3UL, graph.Order());
//   EXPECT_EQ(1UL, graph.Size());
//   EXPECT_TRUE(graph.HasVertex({"x", 0UL, 0UL}));
//   EXPECT_TRUE(graph.HasVertex({"y", 0UL, 1UL}));
//   EXPECT_TRUE(graph.HasVertex({"x", 0UL, 1UL}));
//   EXPECT_TRUE(graph.HasEdge(MakeEdge({"y", 0UL, 1UL}, {"x", 0UL, 1UL})));
//   EXPECT_TRUE(CheckIdentifiers(graph));
// }

// TEST_F(BinderCteUtilTest, BuildTableDependencyGraph6) {
//   // Forward reference x -> y
//   const std::string sql = "WITH x(i) AS (SELECT * FROM y), y(j) AS (SELECT 1) SELECT * FROM x;";
//   auto [_, select] = ParseToSelectStatement(sql);

//   TableDependencyGraph graph{select};
//   EXPECT_EQ(3UL, graph.Order());
//   EXPECT_EQ(1UL, graph.Size());
//   EXPECT_TRUE(graph.HasVertex({"x", 0UL, 0UL}));
//   EXPECT_TRUE(graph.HasVertex({"y", 0UL, 1UL}));
//   EXPECT_TRUE(graph.HasVertex({"y", 0UL, 0UL}));
//   EXPECT_TRUE(graph.HasEdge(MakeEdge({"x", 0UL, 0UL}, {"y", 0UL, 0UL})));
//   EXPECT_TRUE(CheckIdentifiers(graph));
// }

// TEST_F(BinderCteUtilTest, BuildTableDependencyGraph7) {
//   // Forward reference x -> y
//   const std::string sql = "WITH x(i) AS (SELECT j FROM y), y(j) AS (SELECT 1) SELECT * FROM x;";
//   auto [_, select] = ParseToSelectStatement(sql);

//   TableDependencyGraph graph{select};
//   EXPECT_EQ(3UL, graph.Order());
//   EXPECT_EQ(1UL, graph.Size());
//   EXPECT_TRUE(graph.HasVertex({"x", 0UL, 0UL}));
//   EXPECT_TRUE(graph.HasVertex({"y", 0UL, 1UL}));
//   EXPECT_TRUE(graph.HasVertex({"y", 0UL, 0UL}));
//   EXPECT_TRUE(graph.HasEdge(MakeEdge({"x", 0UL, 0UL}, {"y", 0UL, 0UL})));
//   EXPECT_TRUE(CheckIdentifiers(graph));
// }

// TEST_F(BinderCteUtilTest, BuildTableDependencyGraph8) {
//   // Mutual recursion x <-> y
//   const std::string sql = "WITH x(i) AS (SELECT * FROM y), y(j) AS (SELECT * FROM x) SELECT * FROM y;";
//   auto [_, select] = ParseToSelectStatement(sql);

//   TableDependencyGraph graph{select};
//   EXPECT_EQ(4UL, graph.Order());
//   EXPECT_EQ(2UL, graph.Size());
//   EXPECT_TRUE(graph.HasVertex({"x", 0UL, 0UL}));
//   EXPECT_TRUE(graph.HasVertex({"x", 0UL, 1UL}));
//   EXPECT_TRUE(graph.HasVertex({"y", 0UL, 0UL}));
//   EXPECT_TRUE(graph.HasVertex({"y", 0UL, 1UL}));
//   EXPECT_TRUE(graph.HasEdge(MakeEdge({"x", 0UL, 0UL}, {"y", 0UL, 0UL})));
//   EXPECT_TRUE(graph.HasEdge(MakeEdge({"y", 0UL, 1UL}, {"x", 0UL, 1UL})));
//   EXPECT_TRUE(CheckIdentifiers(graph));
// }

// TEST_F(BinderCteUtilTest, BuildTableDependencyGraph9) {
//   // Mutual recursion x <-> y
//   const std::string sql = "WITH x(i) AS (SELECT j FROM y), y(j) AS (SELECT i FROM x) SELECT * FROM y;";
//   auto [_, select] = ParseToSelectStatement(sql);

//   TableDependencyGraph graph{select};
//   EXPECT_EQ(4UL, graph.Order());
//   EXPECT_EQ(2UL, graph.Size());
//   EXPECT_TRUE(graph.HasVertex({"x", 0UL, 0UL}));
//   EXPECT_TRUE(graph.HasVertex({"x", 0UL, 1UL}));
//   EXPECT_TRUE(graph.HasVertex({"y", 0UL, 0UL}));
//   EXPECT_TRUE(graph.HasVertex({"y", 0UL, 1UL}));
//   EXPECT_TRUE(graph.HasEdge(MakeEdge({"x", 0UL, 0UL}, {"y", 0UL, 0UL})));
//   EXPECT_TRUE(graph.HasEdge(MakeEdge({"y", 0UL, 1UL}, {"x", 0UL, 1UL})));
//   EXPECT_TRUE(CheckIdentifiers(graph));
// }

// TEST_F(BinderCteUtilTest, BuildTableDependencyGraph10) {
//   // Nested scope
//   const std::string sql =
//       "WITH x(i) AS (WITH a(m) AS (SELECT 1) SELECT * FROM a), y(j) AS (SELECT * FROM x) SELECT * FROM y;";
//   auto [_, select] = ParseToSelectStatement(sql);

//   TableDependencyGraph graph{select};
//   EXPECT_EQ(5UL, graph.Order());
//   EXPECT_EQ(2UL, graph.Size());
//   EXPECT_TRUE(graph.HasVertex({"x", 0UL, 0UL}));
//   EXPECT_TRUE(graph.HasVertex({"x", 0UL, 1UL}));
//   EXPECT_TRUE(graph.HasVertex({"a", 1UL, 0UL}));
//   EXPECT_TRUE(graph.HasVertex({"a", 0UL, 0UL}));
//   EXPECT_TRUE(graph.HasVertex({"y", 0UL, 1UL}));
//   EXPECT_TRUE(graph.HasEdge(MakeEdge({"x", 0UL, 0UL}, {"a", 0UL, 0UL})));
//   EXPECT_TRUE(graph.HasEdge(MakeEdge({"y", 0UL, 1UL}, {"x", 0UL, 1UL})));
//   EXPECT_TRUE(CheckIdentifiers(graph));
// }

// TEST_F(BinderCteUtilTest, BuildTableDependencyGraph11) {
//   // Nested scope
//   const std::string sql =
//       "WITH x(i) AS (WITH a(m) AS (SELECT 1) SELECT m FROM a), y(j) AS (SELECT * FROM x) SELECT * FROM y;";
//   auto [_, select] = ParseToSelectStatement(sql);

//   TableDependencyGraph graph{select};
//   EXPECT_EQ(5UL, graph.Order());
//   EXPECT_EQ(2UL, graph.Size());
//   EXPECT_TRUE(graph.HasVertex({"x", 0UL, 0UL}));
//   EXPECT_TRUE(graph.HasVertex({"x", 0UL, 1UL}));
//   EXPECT_TRUE(graph.HasVertex({"a", 1UL, 0UL}));
//   EXPECT_TRUE(graph.HasVertex({"a", 0UL, 0UL}));
//   EXPECT_TRUE(graph.HasVertex({"y", 0UL, 1UL}));
//   EXPECT_TRUE(graph.HasEdge(MakeEdge({"x", 0UL, 0UL}, {"a", 0UL, 0UL})));
//   EXPECT_TRUE(graph.HasEdge(MakeEdge({"y", 0UL, 1UL}, {"x", 0UL, 1UL})));
//   EXPECT_TRUE(CheckIdentifiers(graph));
// }

// TEST_F(BinderCteUtilTest, BuildTableDependencyGraph12) {
//   // Nested scope
//   const std::string sql =
//       "WITH x(i) AS (WITH a(m) AS (SELECT 1) SELECT * FROM a), y(j) AS (SELECT i FROM x) SELECT * FROM y;";
//   auto [_, select] = ParseToSelectStatement(sql);

//   TableDependencyGraph graph{select};
//   EXPECT_EQ(5UL, graph.Order());
//   EXPECT_EQ(2UL, graph.Size());
//   EXPECT_TRUE(graph.HasVertex({"x", 0UL, 0UL}));
//   EXPECT_TRUE(graph.HasVertex({"x", 0UL, 1UL}));
//   EXPECT_TRUE(graph.HasVertex({"a", 1UL, 0UL}));
//   EXPECT_TRUE(graph.HasVertex({"a", 0UL, 0UL}));
//   EXPECT_TRUE(graph.HasVertex({"y", 0UL, 1UL}));
//   EXPECT_TRUE(graph.HasEdge(MakeEdge({"x", 0UL, 0UL}, {"a", 0UL, 0UL})));
//   EXPECT_TRUE(graph.HasEdge(MakeEdge({"y", 0UL, 1UL}, {"x", 0UL, 1UL})));
//   EXPECT_TRUE(CheckIdentifiers(graph));
// }

// TEST_F(BinderCteUtilTest, BuildTableDependencyGraph13) {
//   // Nested scope
//   const std::string sql =
//       "WITH x(i) AS (WITH a(m) AS (SELECT 1) SELECT m FROM a), y(j) AS (SELECT i FROM x) SELECT j FROM y;";
//   auto [_, select] = ParseToSelectStatement(sql);

//   TableDependencyGraph graph{select};
//   EXPECT_EQ(5UL, graph.Order());
//   EXPECT_EQ(2UL, graph.Size());
//   EXPECT_TRUE(graph.HasVertex({"x", 0UL, 0UL}));
//   EXPECT_TRUE(graph.HasVertex({"x", 0UL, 1UL}));
//   EXPECT_TRUE(graph.HasVertex({"a", 1UL, 0UL}));
//   EXPECT_TRUE(graph.HasVertex({"a", 0UL, 0UL}));
//   EXPECT_TRUE(graph.HasVertex({"y", 0UL, 1UL}));
//   EXPECT_TRUE(graph.HasEdge(MakeEdge({"x", 0UL, 0UL}, {"a", 0UL, 0UL})));
//   EXPECT_TRUE(graph.HasEdge(MakeEdge({"y", 0UL, 1UL}, {"x", 0UL, 1UL})));
//   EXPECT_TRUE(CheckIdentifiers(graph));
// }

}  // namespace noisepage
