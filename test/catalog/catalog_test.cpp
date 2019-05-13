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
   * Verify the expected catalog tables are present
   * @param db_oid of the database to check
   */
  void VerifyCatalogTables(catalog::db_oid_t db_oid) {
    std::vector<std::string> table_names = {"pg_attribute", "pg_attrdef", "pg_class", "pg_namespace", "pg_type"};
    auto db_h = catalog_->GetDatabaseHandle();
    auto ns_h = db_h.GetNamespaceTable(txn_, db_oid);
    auto ns_oid = ns_h.NameToOid(txn_, "pg_catalog");
    for (auto const &table_name : table_names) {
      VerifyTablePresent(db_oid, ns_oid, table_name.c_str());
    }
  }

  /**
   * Checks pg_class for existence of the specified entry
   * @param db_oid database oid
   * @param ns_oid namespace oid
   * @param table_name
   */
  void VerifyTablePresent(catalog::db_oid_t db_oid, catalog::namespace_oid_t ns_oid, const char *table_name) {
    // verify it is present in pg_class
    auto db_h = catalog_->GetDatabaseHandle();
    auto class_h = db_h.GetClassTable(txn_, db_oid);
    auto class_entry = class_h.GetClassEntry(txn_, ns_oid, table_name);
    EXPECT_NE(nullptr, class_entry);
  }

  /**
   * Checks pg_class for absence of the specified entry
   * @param db_oid database oid
   * @param ns_oid namespace oid
   * @param table_name
   */
  void VerifyTableAbsent(catalog::db_oid_t db_oid, catalog::namespace_oid_t ns_oid, const char *table_name) {
    auto db_h = catalog_->GetDatabaseHandle();
    auto class_h = db_h.GetClassTable(txn_, db_oid);
    auto class_entry = class_h.GetClassEntry(txn_, ns_oid, table_name);
    EXPECT_EQ(nullptr, class_entry);
  }

  catalog::Catalog *catalog_;
  storage::RecordBufferSegmentPool buffer_pool_{100, 100};

  transaction::TransactionContext *txn_ = nullptr;
  transaction::TransactionManager *txn_manager_;
};

/*
 * Create and delete a user table.
 */
// NOLINTNEXTLINE
TEST_F(CatalogTests, UserTableTest) {
  const catalog::db_oid_t default_db_oid(catalog::DEFAULT_DATABASE_OID);
  std::vector<type::TransientValue> row;

  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("id", type::TypeId::INTEGER, false, catalog::col_oid_t(catalog_->GetNextOid()));
  cols.emplace_back("user_col_1", type::TypeId::INTEGER, false, catalog::col_oid_t(catalog_->GetNextOid()));
  catalog::Schema schema(cols);

  // get the namespace oid
  auto db_handle = catalog_->GetDatabaseHandle();
  auto ns_handle = db_handle.GetNamespaceTable(txn_, default_db_oid);
  const std::string ns_public_st("public");
  auto public_ns_oid = ns_handle.NameToOid(txn_, ns_public_st);

  auto tbl_oid = catalog_->CreateUserTable(txn_, default_db_oid, public_ns_oid, "user_table_1", schema);
  VerifyTablePresent(default_db_oid, public_ns_oid, "user_table_1");
  catalog_->Dump(txn_, default_db_oid);

  auto table_p = catalog_->GetUserTable(txn_, default_db_oid, public_ns_oid, tbl_oid);
  row.emplace_back(type::TransientValueFactory::GetInteger(catalog_->GetNextOid()));
  row.emplace_back(type::TransientValueFactory::GetInteger(7));
  table_p->InsertRow(txn_, row);
  table_p->Dump(txn_);

  catalog_->DeleteUserTable(txn_, default_db_oid, public_ns_oid, "user_table_1");
  VerifyTableAbsent(default_db_oid, public_ns_oid, "user_table_1");
  catalog_->Dump(txn_, default_db_oid);
  delete table_p;
}

/*
 * Create and delete a database
 */
// NOLINTNEXTLINE
TEST_F(CatalogTests, DatabaseTest) {
  catalog_->CreateDatabase(txn_, "test_database");
  auto db_handle = catalog_->GetDatabaseHandle();
  auto entry = db_handle.GetDatabaseEntry(txn_, "test_database");
  auto oid = entry->GetOid();
  VerifyCatalogTables(oid);
  catalog_->Dump(txn_, oid);

  // delete it
  catalog_->DeleteDatabase(txn_, "test_database");
}

/*
 * Create and delete a namespace
 */
// NOLINTNEXTLINE
TEST_F(CatalogTests, NamespaceTest) {
  auto db_handle = catalog_->GetDatabaseHandle();
  const catalog::db_oid_t default_db_oid(catalog::DEFAULT_DATABASE_OID);

  auto ns_oid = catalog_->CreateNameSpace(txn_, default_db_oid, "test_namespace");
  auto ns_handle = db_handle.GetNamespaceTable(txn_, default_db_oid);
  auto ns_entry = ns_handle.GetNamespaceEntry(txn_, "test_namespace");
  EXPECT_NE(nullptr, ns_entry);
  EXPECT_EQ("test_namespace", ns_entry->GetVarcharColumn("nspname"));

  catalog_->DeleteNameSpace(txn_, default_db_oid, ns_oid);
  ns_entry = ns_handle.GetNamespaceEntry(txn_, "test_namespace");
  EXPECT_EQ(nullptr, ns_entry);
}

}  // namespace terrier
