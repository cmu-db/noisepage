#include "catalog/catalog.h"
#include <algorithm>
#include <random>
#include <vector>
#include "catalog/catalog_defs.h"
#include "catalog/database_handle.h"
#include "transaction/transaction_manager.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"
namespace terrier {

struct CatalogTests : public TerrierTest {
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

  transaction::TransactionContext *txn_ = nullptr;
  transaction::TransactionManager *txn_manager_;
};

// Tests for higher level catalog API
// NOLINTNEXTLINE
TEST_F(CatalogTests, CreateDatabaseTest) {
  catalog_->CreateDatabase(txn_, "test_database");
  auto db_handle = catalog_->GetDatabaseHandle();
  auto entry = db_handle.GetDatabaseEntry(txn_, "test_database");
  auto oid = entry->GetOid();
  catalog_->Dump(txn_, oid);
}

// NOLINTNEXTLINE
TEST_F(CatalogTests, CreateUserTableTest) {
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);

  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("id", type::TypeId::INTEGER, false, catalog::col_oid_t(catalog_->GetNextOid()));
  cols.emplace_back("user_col_1", type::TypeId::INTEGER, false, catalog::col_oid_t(catalog_->GetNextOid()));
  catalog::Schema schema(cols);

  auto tbl_oid = catalog_->CreateTable(txn_, terrier_oid, "user_table_1", schema);
  catalog_->Dump(txn_, terrier_oid);

  catalog_->DeleteTable(txn_, terrier_oid, tbl_oid);
  catalog_->Dump(txn_, terrier_oid);
}

// NOLINTNEXTLINE
TEST_F(CatalogTests, DeleteDatabaseTest) {
  catalog_->CreateDatabase(txn_, "test_database");
  auto db_handle = catalog_->GetDatabaseHandle();
  auto entry = db_handle.GetDatabaseEntry(txn_, "test_database");
  auto oid = entry->GetOid();
  catalog_->Dump(txn_, oid);

  // delete it
  catalog_->DeleteDatabase(txn_, "test_database");
}

}  // namespace terrier
