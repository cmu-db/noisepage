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
  EXPECT_NE(namespace_entry_ptr, nullptr);
  EXPECT_EQ(namespace_entry_ptr->GetIntColInRow(0), 1012);
  auto nsp_name = namespace_entry_ptr->GetVarcharColInRow(1);
  EXPECT_STREQ(nsp_name, "pg_catalog");
  free(nsp_name);
  // get the public namespace
  namespace_entry_ptr = namespace_handle.GetNamespaceEntry(txn_, "public");
  EXPECT_NE(namespace_entry_ptr, nullptr);
  EXPECT_EQ(namespace_entry_ptr->GetIntColInRow(0), 1013);
  nsp_name = namespace_entry_ptr->GetVarcharColInRow(1);
  EXPECT_STREQ(nsp_name, "public");
  free(nsp_name);
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

  // get the pg_catalog namespace
  auto namespace_entry_ptr = namespace_handle.GetNamespaceEntry(txn_, "test_namespace");
  EXPECT_NE(namespace_entry_ptr, nullptr);
  auto nsp_name = namespace_entry_ptr->GetVarcharColInRow(1);
  EXPECT_STREQ(nsp_name, "test_namespace");
  free(nsp_name);
}
}  // namespace terrier
