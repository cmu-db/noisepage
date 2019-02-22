#include <algorithm>
#include <random>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/namespace_handle.h"
#include "transaction/transaction_manager.h"
#include "util/test_harness.h"
namespace terrier {

struct AttributeHandleTests : public TerrierTest {
  void SetUp() override {
    TerrierTest::SetUp();
    txn_manager_ = new transaction::TransactionManager(&buffer_pool_, true, LOGGING_DISABLED);

    catalog_ = new catalog::Catalog(txn_manager_);
  }

  void TearDown() override {
    TerrierTest::TearDown();
    delete catalog_;  // delete catalog first
    delete txn_manager_;
    delete txn_;
  }

  catalog::Catalog *catalog_;
  storage::RecordBufferSegmentPool buffer_pool_{100, 100};

  transaction::TransactionContext *txn_;
  transaction::TransactionManager *txn_manager_;
};

// Tests that we can get the default namespace and get the correct value from the corresponding row in pg_namespace
// NOLINTNEXTLINE
TEST_F(AttributeHandleTests, BasicCorrectnessTest) {
  txn_ = txn_manager_->BeginTransaction();
  // terrier has db_oid_t DEFAULT_DATABASE_OID
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
  auto db_handle = catalog_->GetDatabaseHandle();
  auto table_handle = db_handle.GetNamespaceHandle(txn_, terrier_oid).GetTableHandle(txn_, "pg_catalog");
  auto attribute_handle = table_handle.GetAttributeHandle(txn_, "pg_database");

  // pg_database has columns: oid | datname
  auto attribute_entry_ptr = attribute_handle.GetAttributeEntry(txn_, "oid");
  EXPECT_NE(attribute_entry_ptr, nullptr);

  // the oid should belongs to pg_database table
  uint32_t rel_id = attribute_entry_ptr->GetColumn(1).GetIntValue();
  EXPECT_EQ(rel_id, !table_handle.NameToOid(txn_, "pg_database"));

  // pg_database doesn't have column "attrelid". Searching for such column should result in an exception.
  EXPECT_THROW(attribute_handle.GetAttributeEntry(txn_, "attrlid"), CatalogException);
}
}  // namespace terrier
