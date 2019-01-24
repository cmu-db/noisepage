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

// Tests that the catalog contains the default database.
// NOLINTNEXTLINE
TEST_F(DatabaseHandleTests, BasicCorrectnessTest) {
  // the oid of the default database, the global catalog of all databases
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);

  // the handle provides accessors to the database
  catalog::DatabaseHandle db_handle = catalog_->GetDatabaseHandle(terrier_oid);

  txn_ = txn_manager_->BeginTransaction();
  // lookup the default database
  // auto db_entry_ptr = db_handle.GetDatabaseEntry(txn_, terrier_oid);
  auto db_entry_ptr = db_handle.GetDatabaseEntry(txn_, terrier_oid);

  // must get back an entry
  EXPECT_NE(db_entry_ptr, nullptr);
  auto db_oid = db_entry_ptr->GetIntColInRow(0);
  EXPECT_EQ(db_oid, !terrier_oid);
  // column 2 is the database name.
  // TODO(pakhtar): fix to be of correct type and value once we have varlen support
  auto db_name_val = db_entry_ptr->GetIntColInRow(1);
  EXPECT_EQ(db_name_val, 12345);
}
}  // namespace terrier
