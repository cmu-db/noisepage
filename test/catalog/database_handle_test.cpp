#include "catalog/database_handle.h"
#include <algorithm>
#include <random>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"
#include "transaction/transaction_manager.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"
namespace terrier {

struct DatabaseHandleTests : public TerrierTest {
  void SetUp() override {
    TerrierTest::SetUp();
    txn_manager_ = new transaction::TransactionManager(&buffer_pool_, true, LOGGING_DISABLED);

    txn_ = txn_manager_->BeginTransaction();
    catalog_ = new catalog::Catalog(txn_manager_, txn_);
  }

  void TearDown() override {
    txn_manager_->Commit(txn_, TestCallbacks::EmptyCallback, nullptr);

    TerrierTest::TearDown();
    delete catalog_;  // need to delete catalog_first
    delete txn_manager_;
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
  catalog::DatabaseCatalogTable db_handle = catalog_->GetDatabaseHandle();

  // lookup the default database
  auto db_entry_ptr = db_handle.GetDatabaseEntry(txn_, terrier_oid);

  EXPECT_EQ(!terrier_oid, type::TransientValuePeeker::PeekInteger(db_entry_ptr->GetColumn(0)));
  EXPECT_EQ("terrier", type::TransientValuePeeker::PeekVarChar(db_entry_ptr->GetColumn(1)));
}

// NOLINTNEXTLINE
TEST_F(DatabaseHandleTests, BasicEntryTest) {
  // the oid of the default database, the global catalog of all databases
  // const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);

  // the handle provides accessors to the database
  catalog::DatabaseCatalogTable db_handle = catalog_->GetDatabaseHandle();

  // check absence
  auto no_entry_p = db_handle.GetDatabaseEntry(txn_, "test_db");
  EXPECT_EQ(nullptr, no_entry_p);

  // create an entry
  catalog_->CreateDatabase(txn_, "test_db");

  // check existence
  auto test_entry_p = db_handle.GetDatabaseEntry(txn_, "test_db");
  EXPECT_NE(nullptr, test_entry_p);

  // delete the entry
  // test_entry_p->Delete(txn_);
  catalog_->DeleteDatabase(txn_, "test_db");

  // check absence
  no_entry_p = db_handle.GetDatabaseEntry(txn_, "test_db");
  EXPECT_EQ(nullptr, no_entry_p);
}
}  // namespace terrier
