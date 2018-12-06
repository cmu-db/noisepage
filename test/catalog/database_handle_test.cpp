#include "catalog/database_handle.h"
#include <algorithm>
#include <random>
#include <vector>
#include "catalog/catalog.h"
#include "transaction/transaction_manager.h"
#include "util/test_harness.h"
namespace terrier {

struct DatabaseHandleTests : public TerrierTest {
  void SetUp() override {
    TerrierTest::SetUp();
    // create a fake transaction
    txn_manager_ = new transaction::TransactionManager(&buffer_pool_, true, LOGGING_DISABLED);

    catalog_ = new catalog::Catalog(txn_manager_);
  }

  void TearDown() override {
    TerrierTest::TearDown();
    delete txn_manager_;
    delete catalog_;
  }

  catalog::Catalog *catalog_;
  storage::RecordBufferSegmentPool buffer_pool_{100, 100};

  transaction::TransactionContext *txn_;
  transaction::TransactionManager *txn_manager_;
};

// NOLINTNEXTLINE
TEST_F(DatabaseHandleTests, DatabaseEntryTest) {
  txn_ = txn_manager_->BeginTransaction();
  // terrier has db_oid_t 0
  const catalog::oid_t terrier_oid = catalog::oid_t(0);
  catalog::DatabaseHandle db_handle = catalog_->GetDatabase(terrier_oid);
  auto db_entry_ptr = db_handle.GetDatabaseEntry(txn_, terrier_oid);
  EXPECT_TRUE(db_entry_ptr != nullptr);
  // test if we are getting the correct value

  // oid has col_oid_t = 1
  EXPECT_TRUE(*reinterpret_cast<uint32_t *>(db_entry_ptr->GetValue(catalog::col_oid_t(1))) == 0);
  // datname has col_oid_t = 2
  EXPECT_TRUE(*reinterpret_cast<uint32_t *>(db_entry_ptr->GetValue(catalog::col_oid_t(2))) == 15721);
  delete txn_;
}

}  // namespace terrier
