#include "catalog/namespace_handle.h"
#include <algorithm>
#include <random>
#include <vector>
#include "catalog/catalog.h"
#include "transaction/transaction_manager.h"
#include "util/test_harness.h"
namespace terrier {

struct NamespaceHandleTests : public TerrierTest {
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
TEST_F(NamespaceHandleTests, BasicCorrectnessTest) {
  txn_ = txn_manager_->BeginTransaction();
  // terrier has db_oid_t DEFAULT_DATABASE_OID
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
  auto db_handle = catalog_->GetDatabaseHandle();
  auto namespace_handle = db_handle.GetNamespaceHandle(txn_, terrier_oid);

  // get the pg_catalog namespace
  auto namespace_entry_ptr = namespace_handle.GetNamespaceEntry(txn_, "pg_catalog");

  /* if we want to check the oid value, we need a way to do it so we have
   * access to the generated value. Hardcoded, it is very fragile.
   */
  // EXPECT_EQ(1012, namespace_entry_ptr->GetColumn(0).GetIntValue());
  EXPECT_STREQ("pg_catalog", namespace_entry_ptr->GetColumn(1).GetVarcharValue());
  // get the public namespace
  namespace_entry_ptr = namespace_handle.GetNamespaceEntry(txn_, "public");

  // EXPECT_EQ(1013, namespace_entry_ptr->GetColumn(0).GetIntValue());
  EXPECT_STREQ("public", namespace_entry_ptr->GetColumn(1).GetVarcharValue());
}

// Tests that we can create namespace
// NOLINTNEXTLINE
TEST_F(NamespaceHandleTests, CreateTest) {
  txn_ = txn_manager_->BeginTransaction();
  // terrier has db_oid_t DEFAULT_DATABASE_OID
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
  auto db_handle = catalog_->GetDatabaseHandle();
  auto namespace_handle = db_handle.GetNamespaceHandle(txn_, terrier_oid);
  namespace_handle.CreateNamespace(txn_, "test_namespace");

  // verify correctly created
  auto namespace_entry_ptr = namespace_handle.GetNamespaceEntry(txn_, "test_namespace");
  EXPECT_STREQ("test_namespace", namespace_entry_ptr->GetColumn(1).GetVarcharValue());
}
}  // namespace terrier
