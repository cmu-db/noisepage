#include "catalog/namespace_handle.h"
#include <algorithm>
#include <random>
#include <vector>
#include "catalog/catalog.h"
#include "transaction/transaction_manager.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"
namespace terrier {

struct NamespaceHandleTests : public TerrierTest {
  void SetUp() override {
    TerrierTest::SetUp();
    txn_manager_ = new transaction::TransactionManager(&buffer_pool_, true, LOGGING_DISABLED);

    txn_ = txn_manager_->BeginTransaction();
    catalog_ = new catalog::Catalog(txn_manager_, txn_);
  }

  void TearDown() override {
    txn_manager_->Commit(txn_, TestCallbacks::EmptyCallback, nullptr);

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
  // terrier has db_oid_t DEFAULT_DATABASE_OID
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
  auto db_handle = catalog_->GetDatabaseHandle();
  auto namespace_handle = db_handle.GetNamespaceTable(txn_, terrier_oid);

  // get the pg_catalog namespace
  auto namespace_entry_ptr = namespace_handle.GetNamespaceEntry(txn_, "pg_catalog");
  // EXPECT_EQ("pg_catalog", type::TransientValuePeeker::PeekVarChar(namespace_entry_ptr->GetColumn(1)));
  EXPECT_EQ("pg_catalog", namespace_entry_ptr->GetVarcharColumn("nspname"));

  // get the public namespace
  namespace_entry_ptr = namespace_handle.GetNamespaceEntry(txn_, "public");
  EXPECT_EQ("public", namespace_entry_ptr->GetVarcharColumn("nspname"));
  // EXPECT_EQ("public", type::TransientValuePeeker::PeekVarChar(namespace_entry_ptr->GetColumn(1)));
}

// Tests that we can create namespace
// NOLINTNEXTLINE
TEST_F(NamespaceHandleTests, CreateTest) {
  // terrier has db_oid_t DEFAULT_DATABASE_OID
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
  auto db_handle = catalog_->GetDatabaseHandle();
  auto namespace_handle = db_handle.GetNamespaceTable(txn_, terrier_oid);
  namespace_handle.AddEntry(txn_, "test_namespace");

  // verify correctly created
  auto namespace_entry_ptr = namespace_handle.GetNamespaceEntry(txn_, "test_namespace");
  EXPECT_EQ("test_namespace", namespace_entry_ptr->GetVarcharColumn("nspname"));

  // test deletion
  namespace_handle.DeleteEntry(txn_, namespace_entry_ptr);
  namespace_entry_ptr = namespace_handle.GetNamespaceEntry(txn_, "test_namespace");
  EXPECT_EQ(nullptr, namespace_entry_ptr);
}

// Test oid lookup
// NOLINTNEXTLINE
TEST_F(NamespaceHandleTests, OidLookupTest) {
  // terrier has db_oid_t DEFAULT_DATABASE_OID
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
  auto db_handle = catalog_->GetDatabaseHandle();
  auto namespace_handle = db_handle.GetNamespaceTable(txn_, terrier_oid);
  namespace_handle.AddEntry(txn_, "test_namespace");

  // verify that we can get to an oid
  auto UNUSED_ATTRIBUTE ns_oid = namespace_handle.NameToOid(txn_, "test_namespace");

  // delete it
  auto namespace_entry_ptr = namespace_handle.GetNamespaceEntry(txn_, "test_namespace");
  namespace_handle.DeleteEntry(txn_, namespace_entry_ptr);
  EXPECT_THROW(namespace_handle.NameToOid(txn_, "test_namespace"), CatalogException);
}

}  // namespace terrier
