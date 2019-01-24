#include <algorithm>
#include <random>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/namespace_handle.h"
#include "transaction/transaction_manager.h"
#include "util/test_harness.h"
namespace terrier {

struct TablespaceHandleTests : public TerrierTest {
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
TEST_F(TablespaceHandleTests, BasicCorrectnessTest) {
  txn_ = txn_manager_->BeginTransaction();
  // terrier has db_oid_t DEFAULT_DATABASE_OID
  auto tsp_handle = catalog_->GetTablespaceHandle();
  auto tsp_entry_ptr = tsp_handle.GetTablespaceEntry(txn_, "pg_global");
  EXPECT_NE(tsp_entry_ptr, nullptr);
  // test if we are getting the correct value
  // oid has col_oid_t = 1012
  EXPECT_EQ(*reinterpret_cast<uint32_t *>(tsp_entry_ptr->GetValue("oid")), 1007);
}
}  // namespace terrier
