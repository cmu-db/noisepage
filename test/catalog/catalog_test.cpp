#include "catalog/catalog.h"
#include <algorithm>
#include <random>
#include <string>
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

  /**
   * Verify the expected tables are present
   * @param db_oid of the database to check
   */
  void VerifyTables(catalog::db_oid_t db_oid) {
    std::vector<std::string> table_names = {"pg_attribute", "pg_attrdef", "pg_class", "pg_namespace", "pg_type"};
    for (auto const &table_name : table_names) {
      VerifyTablePresent(db_oid, table_name.c_str());
    }
  }

  void VerifyTablePresent(catalog::db_oid_t db_oid, const char *table_name) {
    // verify that we can get to the table
    auto table_p = catalog_->GetCatalogTable(db_oid, table_name);

    // verify it is present in pg_class
    auto db_h = catalog_->GetDatabaseHandle();
    auto class_h = db_h.GetClassHandle(txn_, db_oid);
    auto class_entry = class_h.GetClassEntry(txn_, table_name);
  }

  void VerifyTableAbsent(catalog::db_oid_t db_oid, const char *table_name) {
    EXPECT_THROW(catalog_->GetCatalogTable(db_oid, table_name), std::out_of_range);
    // TODO(pakhtar): verify absence from pg_class
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

  // that we can retrieve the entry means is was correctly inserted into the catalog
  auto entry = db_handle.GetDatabaseEntry(txn_, "test_database");
  auto oid = entry->GetOid();
  VerifyTables(oid);
  catalog_->Dump(txn_, oid);
}

// NOLINTNEXTLINE
TEST_F(CatalogTests, CreateUserTableTest) {
  const catalog::db_oid_t default_db_oid(catalog::DEFAULT_DATABASE_OID);
  std::vector<type::TransientValue> row;

  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("id", type::TypeId::INTEGER, false, catalog::col_oid_t(catalog_->GetNextOid()));
  cols.emplace_back("user_col_1", type::TypeId::INTEGER, false, catalog::col_oid_t(catalog_->GetNextOid()));
  catalog::Schema schema(cols);

  auto tbl_oid = catalog_->CreateUserTable(txn_, default_db_oid, "user_table_1", schema);
  VerifyTablePresent(default_db_oid, "user_table_1");
  catalog_->Dump(txn_, default_db_oid);

  auto table_p = catalog_->GetCatalogTable(default_db_oid, tbl_oid);
  // get the table ptr
  row.emplace_back(type::TransientValueFactory::GetInteger(catalog_->GetNextOid()));
  row.emplace_back(type::TransientValueFactory::GetInteger(7));
  table_p->InsertRow(txn_, row);
  table_p->Dump(txn_);

  catalog_->DeleteTable(txn_, default_db_oid, "user_table_1");
  VerifyTableAbsent(default_db_oid, "user_table_1");
  catalog_->Dump(txn_, default_db_oid);
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
