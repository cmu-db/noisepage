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
    delete txn_manager_;
    delete catalog_;
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
  // terrier has db_oid_t 0
  const catalog::db_oid_t terrier_oid(0);
  auto db_handle = catalog_->GetDatabaseHandle(terrier_oid);
  auto nsp_handle = db_handle.GetNamespaceHandle();
  // get the pg_catalog namespace
  catalog::nsp_oid_t pg_catalog_oid(0);
  auto nsp_entry_ptr = nsp_handle.GetNamespaceEntry(txn_, pg_catalog_oid);
  EXPECT_TRUE(nsp_entry_ptr != nullptr);
  // test if we are getting the correct value
  // oid has col_oid_t = 1000
  EXPECT_TRUE(*reinterpret_cast<uint32_t *>(nsp_entry_ptr->GetValue(catalog::col_oid_t(1000))) == !pg_catalog_oid);
  // datname has col_oid_t = 1001
  EXPECT_TRUE(*reinterpret_cast<uint32_t *>(nsp_entry_ptr->GetValue(catalog::col_oid_t(1001))) == 22222);
}
}  // namespace terrier
