#include <memory>

#include "binder/bind_node_visitor.h"
#include "binder/cte/structured_statement.h"
#include "catalog/catalog.h"
#include "common/managed_pointer.h"
#include "main/db_main.h"
#include "parser/postgresparser.h"
#include "parser/statements.h"
#include "test_util/binder_test_util.h"
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
    auto int_default = parser::ConstantValueExpression(execution::sql::SqlTypeId::Integer);

    // Create table
    txn_ = txn_manager_->BeginTransaction();
    accessor_ = catalog_->GetAccessor(common::ManagedPointer(txn_), db_oid_, DISABLED);

    // CREATE TABLE TEST(x INT, y INT)
    std::vector<catalog::Schema::Column> cols{};
    cols.emplace_back("x", execution::sql::SqlTypeId::Integer, true, int_default);
    cols.emplace_back("y", execution::sql::SqlTypeId::Integer, true, int_default);
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

TEST_F(BinderCteStructuredStatementTest, ItWorks) {
  const std::string sql = "SELECT * FROM TestTable;";
  auto [_, select] = BinderTestUtil::ParseToSelectStatement(sql);
  EXPECT_EQ(select->GetDepth(), -1);
  EXPECT_TRUE(select->HasSelectTable());
  EXPECT_EQ(select->GetSelectTable()->GetAlias().GetName(), "testtable");
}

TEST_F(BinderCteStructuredStatementTest, BuildStatement0) {
  const std::string sql = "SELECT * FROM TestTable;";
  auto [_, select] = BinderTestUtil::ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  EXPECT_EQ(1UL, statement.RefCount());
  EXPECT_EQ(1UL, statement.ScopeCount());
  EXPECT_TRUE(statement.HasRef({"testtable", 0UL, 0UL}));
}

TEST_F(BinderCteStructuredStatementTest, BuildStatement1) {
  const std::string sql = "WITH x(i) AS (SELECT 1) SELECT * FROM x;";
  auto [_, select] = BinderTestUtil::ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  EXPECT_EQ(2UL, statement.RefCount());
  EXPECT_EQ(2UL, statement.ScopeCount());
  EXPECT_TRUE(statement.HasWriteRef({"x", 0UL, 0UL}));
  EXPECT_TRUE(statement.HasReadRef({"x", 0UL, 1UL}));
}

TEST_F(BinderCteStructuredStatementTest, BuildStatement2) {
  const std::string sql = "WITH x(i) AS (SELECT 1), y(j) AS (SELECT 1) SELECT * FROM y;";
  auto [_, select] = BinderTestUtil::ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  EXPECT_EQ(3UL, statement.RefCount());
  EXPECT_EQ(3UL, statement.ScopeCount());
  EXPECT_TRUE(statement.HasWriteRef({"x", 0UL, 0UL}));
  EXPECT_TRUE(statement.HasWriteRef({"y", 0UL, 1UL}));
  EXPECT_TRUE(statement.HasReadRef({"y", 0UL, 2UL}));
}

TEST_F(BinderCteStructuredStatementTest, BuildStatement3) {
  const std::string sql = "WITH x(i) AS (SELECT 1), y(j) AS (SELECT 1), z(k) AS (SELECT 1) SELECT * FROM z;";
  auto [_, select] = BinderTestUtil::ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  EXPECT_EQ(4UL, statement.RefCount());
  EXPECT_EQ(4UL, statement.ScopeCount());
  EXPECT_TRUE(statement.HasWriteRef({"x", 0UL, 0UL}));
  EXPECT_TRUE(statement.HasWriteRef({"y", 0UL, 1UL}));
  EXPECT_TRUE(statement.HasWriteRef({"z", 0UL, 2UL}));
  EXPECT_TRUE(statement.HasReadRef({"z", 0UL, 3UL}));
}

TEST_F(BinderCteStructuredStatementTest, BuildStatement4) {
  const std::string sql = "WITH x(i) AS (SELECT 1), y(j) AS (SELECT * FROM x) SELECT * FROM y;";
  auto [_, select] = BinderTestUtil::ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  EXPECT_EQ(4UL, statement.RefCount());
  EXPECT_EQ(3UL, statement.ScopeCount());
  EXPECT_TRUE(statement.HasWriteRef({"x", 0UL, 0UL}));
  EXPECT_TRUE(statement.HasWriteRef({"y", 0UL, 1UL}));
  EXPECT_TRUE(statement.HasReadRef({"y", 0UL, 2UL}));
  EXPECT_TRUE(statement.HasReadRef({"x", 1UL, 0UL}));
}

TEST_F(BinderCteStructuredStatementTest, BuildStatement5) {
  const std::string sql = "WITH x(i) AS (SELECT 1), y(j) AS (SELECT i FROM x) SELECT * FROM y;";
  auto [_, select] = BinderTestUtil::ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  EXPECT_EQ(4UL, statement.RefCount());
  EXPECT_EQ(3UL, statement.ScopeCount());
  EXPECT_TRUE(statement.HasWriteRef({"x", 0UL, 0UL}));
  EXPECT_TRUE(statement.HasWriteRef({"y", 0UL, 1UL}));
  EXPECT_TRUE(statement.HasReadRef({"y", 0UL, 2UL}));
  EXPECT_TRUE(statement.HasReadRef({"x", 1UL, 0UL}));
}

TEST_F(BinderCteStructuredStatementTest, BuildStatement6) {
  const std::string sql = "WITH x(i) AS (SELECT * FROM y), y(j) AS (SELECT 1) SELECT * FROM x;";
  auto [_, select] = BinderTestUtil::ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  EXPECT_EQ(4UL, statement.RefCount());
  EXPECT_EQ(3UL, statement.ScopeCount());
  EXPECT_TRUE(statement.HasWriteRef({"x", 0UL, 0UL}));
  EXPECT_TRUE(statement.HasWriteRef({"y", 0UL, 1UL}));
  EXPECT_TRUE(statement.HasReadRef({"x", 0UL, 2UL}));
  EXPECT_TRUE(statement.HasReadRef({"y", 1UL, 0UL}));
}

TEST_F(BinderCteStructuredStatementTest, BuildStatement7) {
  const std::string sql = "WITH x(i) AS (SELECT j FROM y), y(j) AS (SELECT 1) SELECT * FROM x;";
  auto [_, select] = BinderTestUtil::ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  EXPECT_EQ(4UL, statement.RefCount());
  EXPECT_EQ(3UL, statement.ScopeCount());
  EXPECT_TRUE(statement.HasWriteRef({"x", 0UL, 0UL}));
  EXPECT_TRUE(statement.HasWriteRef({"y", 0UL, 1UL}));
  EXPECT_TRUE(statement.HasReadRef({"x", 0UL, 2UL}));
  EXPECT_TRUE(statement.HasReadRef({"y", 1UL, 0UL}));
}

TEST_F(BinderCteStructuredStatementTest, BuildStatement8) {
  const std::string sql = "WITH x(i) AS (SELECT * FROM y), y(j) AS (SELECT * FROM x) SELECT * FROM y;";
  auto [_, select] = BinderTestUtil::ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  EXPECT_EQ(5UL, statement.RefCount());
  EXPECT_EQ(3UL, statement.ScopeCount());
  EXPECT_TRUE(statement.HasWriteRef({"x", 0UL, 0UL}));
  EXPECT_TRUE(statement.HasWriteRef({"y", 0UL, 1UL}));
  EXPECT_TRUE(statement.HasReadRef({"y", 0UL, 2UL}));
  EXPECT_TRUE(statement.HasReadRef({"y", 1UL, 0UL}));
  EXPECT_TRUE(statement.HasReadRef({"x", 1UL, 0UL}));
}

TEST_F(BinderCteStructuredStatementTest, BuildStatement9) {
  const std::string sql = "WITH x(i) AS (SELECT j FROM y), y(j) AS (SELECT i FROM x) SELECT * FROM y;";
  auto [_, select] = BinderTestUtil::ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  EXPECT_EQ(5UL, statement.RefCount());
  EXPECT_EQ(3UL, statement.ScopeCount());
  EXPECT_TRUE(statement.HasWriteRef({"x", 0UL, 0UL}));
  EXPECT_TRUE(statement.HasWriteRef({"y", 0UL, 1UL}));
  EXPECT_TRUE(statement.HasReadRef({"y", 0UL, 2UL}));
  EXPECT_TRUE(statement.HasReadRef({"y", 1UL, 0UL}));
  EXPECT_TRUE(statement.HasReadRef({"x", 1UL, 0UL}));
}

TEST_F(BinderCteStructuredStatementTest, BuildStatement10) {
  const std::string sql =
      "WITH x(i) AS (WITH a(m) AS (SELECT 1) SELECT * FROM a), y(j) AS (SELECT * FROM x) SELECT * FROM y;";
  auto [_, select] = BinderTestUtil::ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  EXPECT_EQ(6UL, statement.RefCount());
  EXPECT_EQ(4UL, statement.ScopeCount());

  EXPECT_TRUE(statement.HasWriteRef({"x", 0UL, 0UL}));
  EXPECT_TRUE(statement.HasWriteRef({"y", 0UL, 1UL}));
  EXPECT_TRUE(statement.HasReadRef({"y", 0UL, 2UL}));

  EXPECT_TRUE(statement.HasWriteRef({"a", 1UL, 0UL}));
  EXPECT_TRUE(statement.HasReadRef({"a", 1UL, 1UL}));

  EXPECT_TRUE(statement.HasReadRef({"x", 1UL, 0UL}));
}

TEST_F(BinderCteStructuredStatementTest, BuildStatement11) {
  const std::string sql =
      "WITH x(i) AS (WITH a(m) AS (SELECT 1) SELECT m FROM a), y(j) AS (SELECT * FROM x) SELECT * FROM y;";
  auto [_, select] = BinderTestUtil::ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  EXPECT_EQ(6UL, statement.RefCount());
  EXPECT_EQ(4UL, statement.ScopeCount());

  EXPECT_TRUE(statement.HasWriteRef({"x", 0UL, 0UL}));
  EXPECT_TRUE(statement.HasWriteRef({"y", 0UL, 1UL}));
  EXPECT_TRUE(statement.HasReadRef({"y", 0UL, 2UL}));

  EXPECT_TRUE(statement.HasWriteRef({"a", 1UL, 0UL}));
  EXPECT_TRUE(statement.HasReadRef({"a", 1UL, 1UL}));

  EXPECT_TRUE(statement.HasReadRef({"x", 1UL, 0UL}));
}

TEST_F(BinderCteStructuredStatementTest, BuildStatement12) {
  const std::string sql =
      "WITH x(i) AS (WITH a(m) AS (SELECT 1) SELECT * FROM a), y(j) AS (SELECT i FROM x) SELECT * FROM y;";
  auto [_, select] = BinderTestUtil::ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  EXPECT_EQ(6UL, statement.RefCount());
  EXPECT_EQ(4UL, statement.ScopeCount());

  EXPECT_TRUE(statement.HasWriteRef({"x", 0UL, 0UL}));
  EXPECT_TRUE(statement.HasWriteRef({"y", 0UL, 1UL}));
  EXPECT_TRUE(statement.HasReadRef({"y", 0UL, 2UL}));

  EXPECT_TRUE(statement.HasWriteRef({"a", 1UL, 0UL}));
  EXPECT_TRUE(statement.HasReadRef({"a", 1UL, 1UL}));

  EXPECT_TRUE(statement.HasReadRef({"x", 1UL, 0UL}));
}

TEST_F(BinderCteStructuredStatementTest, BuildStatement13) {
  const std::string sql =
      "WITH x(i) AS (WITH a(m) AS (SELECT 1) SELECT m FROM a), y(j) AS (SELECT i FROM x) SELECT j FROM y;";
  auto [_, select] = BinderTestUtil::ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  EXPECT_EQ(6UL, statement.RefCount());
  EXPECT_EQ(4UL, statement.ScopeCount());

  EXPECT_TRUE(statement.HasWriteRef({"x", 0UL, 0UL}));
  EXPECT_TRUE(statement.HasWriteRef({"y", 0UL, 1UL}));
  EXPECT_TRUE(statement.HasReadRef({"y", 0UL, 2UL}));

  EXPECT_TRUE(statement.HasWriteRef({"a", 1UL, 0UL}));
  EXPECT_TRUE(statement.HasReadRef({"a", 1UL, 1UL}));

  EXPECT_TRUE(statement.HasReadRef({"x", 1UL, 0UL}));
}

TEST_F(BinderCteStructuredStatementTest, BuildStatement14) {
  const std::string sql = "WITH x(i) AS (SELECT 1 UNION SELECT 2) SELECT * FROM x;";
  auto [_, select] = BinderTestUtil::ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  EXPECT_EQ(2UL, statement.RefCount());
  EXPECT_EQ(2UL, statement.ScopeCount());

  EXPECT_TRUE(statement.HasWriteRef({"x", 0UL, 0UL}));
  EXPECT_TRUE(statement.HasReadRef({"x", 0UL, 1UL}));
}

TEST_F(BinderCteStructuredStatementTest, BuildStatement15) {
  const std::string sql = "WITH x(i) AS (SELECT 1 UNION ALL SELECT 2) SELECT * FROM x;";
  auto [_, select] = BinderTestUtil::ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  EXPECT_EQ(2UL, statement.RefCount());
  EXPECT_EQ(2UL, statement.ScopeCount());

  EXPECT_TRUE(statement.HasWriteRef({"x", 0UL, 0UL}));
  EXPECT_TRUE(statement.HasReadRef({"x", 0UL, 1UL}));
}

TEST_F(BinderCteStructuredStatementTest, BuildStatement16) {
  const std::string sql = "WITH RECURSIVE x(i) AS (SELECT 1 UNION SELECT i FROM x WHERE i < 5) SELECT * FROM x;";
  auto [_, select] = BinderTestUtil::ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  EXPECT_EQ(3UL, statement.RefCount());
  EXPECT_EQ(2UL, statement.ScopeCount());

  EXPECT_TRUE(statement.HasWriteRef({"x", 0UL, 0UL}));
  EXPECT_TRUE(statement.HasReadRef({"x", 0UL, 1UL}));
  EXPECT_TRUE(statement.HasReadRef({"x", 1UL, 0UL}));
}

TEST_F(BinderCteStructuredStatementTest, BuildStatement17) {
  const std::string sql = "WITH RECURSIVE x(i) AS (SELECT 1 UNION ALL SELECT i FROM x WHERE i < 5) SELECT * FROM x;";
  auto [_, select] = BinderTestUtil::ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  EXPECT_EQ(3UL, statement.RefCount());
  EXPECT_EQ(2UL, statement.ScopeCount());

  EXPECT_TRUE(statement.HasWriteRef({"x", 0UL, 0UL}));
  EXPECT_TRUE(statement.HasReadRef({"x", 0UL, 1UL}));
  EXPECT_TRUE(statement.HasReadRef({"x", 1UL, 0UL}));
}

TEST_F(BinderCteStructuredStatementTest, BuildStatement18) {
  const std::string sql =
      "WITH RECURSIVE x(i) AS (SELECT 1 UNION ALL SELECT i FROM x WHERE i < 5), y(j) AS (SELECT * FROM x) SELECT * "
      "FROM y;";
  auto [_, select] = BinderTestUtil::ParseToSelectStatement(sql);

  StructuredStatement statement{select};
  EXPECT_EQ(5UL, statement.RefCount());
  EXPECT_EQ(3UL, statement.ScopeCount());

  EXPECT_TRUE(statement.HasWriteRef({"x", 0UL, 0UL}));
  EXPECT_TRUE(statement.HasWriteRef({"y", 0UL, 1UL}));
  EXPECT_TRUE(statement.HasReadRef({"y", 0UL, 2UL}));

  // Same alias, identical relative positions within scopes
  EXPECT_EQ(2UL, statement.ReadRefCount({"x", 1UL, 0UL}));
}

}  // namespace noisepage
