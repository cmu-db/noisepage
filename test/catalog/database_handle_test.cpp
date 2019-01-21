#include "catalog/database_handle.h"
#include <algorithm>
#include <random>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"
#include "transaction/transaction_manager.h"
#include "util/test_harness.h"
namespace terrier {

struct DatabaseHandleTests : public TerrierTest {
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

// Tests that we can get the default database and get the correct value from the corresponding row in pg_database
// NOLINTNEXTLINE
TEST_F(DatabaseHandleTests, BasicCorrectnessTest) {
  txn_ = txn_manager_->BeginTransaction();
  // terrier has db_oid_t DEFAULT_DATABASE_OID
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
  catalog::DatabaseHandle db_handle = catalog_->GetDatabaseHandle(terrier_oid);
  auto db_entry_ptr = db_handle.GetDatabaseEntry(txn_, terrier_oid);
  EXPECT_NE(db_entry_ptr, nullptr);
  // test if we are getting the correct value
  // oid has col_oid_t = 1002
  EXPECT_EQ(*reinterpret_cast<uint32_t *>(db_entry_ptr->GetValue("oid")), !terrier_oid);
  // datname has col_oid_t = 1003
  EXPECT_EQ(*reinterpret_cast<uint32_t *>(db_entry_ptr->GetValue("datname")), 12345);
}
}  // namespace terrier
