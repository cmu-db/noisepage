#include <algorithm>
#include <random>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/namespace_handle.h"
#include "catalog/schema.h"
#include "transaction/transaction_manager.h"
#include "util/test_harness.h"
namespace terrier {

struct TableHandleTests : public TerrierTest {
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
TEST_F(TableHandleTests, BasicCorrectnessTest) {
  txn_ = txn_manager_->BeginTransaction();
  // terrier has db_oid_t DEFAULT_DATABASE_OID
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
  auto db_handle = catalog_->GetDatabaseHandle();
  auto table_handle = db_handle.GetNamespaceHandle(txn_, terrier_oid).GetTableHandle(txn_, "pg_catalog");

  // test if get correct tablename
  auto table_entry_ptr = table_handle.GetTableEntry(txn_, "pg_database");
  EXPECT_NE(table_entry_ptr, nullptr);
  auto str = table_entry_ptr->GetVarcharColInRow(1);
  EXPECT_STREQ(str, "pg_database");
  free(str);

  table_entry_ptr = table_handle.GetTableEntry(txn_, "pg_tablespace");
  EXPECT_NE(table_entry_ptr, nullptr);
  str = table_entry_ptr->GetVarcharColInRow(1);
  EXPECT_STREQ(str, "pg_tablespace");
  free(str);

  table_entry_ptr = table_handle.GetTableEntry(txn_, "pg_namespace");
  EXPECT_NE(table_entry_ptr, nullptr);
  str = table_entry_ptr->GetVarcharColInRow(1);
  EXPECT_STREQ(str, "pg_namespace");
  free(str);

  table_entry_ptr = table_handle.GetTableEntry(txn_, "pg_class");
  EXPECT_NE(table_entry_ptr, nullptr);
  str = table_entry_ptr->GetVarcharColInRow(1);
  LOG_INFO("LALAL {}", str);
  EXPECT_STREQ(str, "pg_class");
  free(str);

  // test if get correct schemaname (namespace)
  table_entry_ptr = table_handle.GetTableEntry(txn_, "pg_database");
  EXPECT_NE(table_entry_ptr, nullptr);
  str = table_entry_ptr->GetVarcharColInRow(0);
  EXPECT_STREQ(str, "pg_catalog");
  free(str);

  table_entry_ptr = table_handle.GetTableEntry(txn_, "pg_tablespace");
  EXPECT_NE(table_entry_ptr, nullptr);
  str = table_entry_ptr->GetVarcharColInRow(0);
  EXPECT_STREQ(str, "pg_catalog");
  free(str);

  table_entry_ptr = table_handle.GetTableEntry(txn_, "pg_namespace");
  EXPECT_NE(table_entry_ptr, nullptr);
  str = table_entry_ptr->GetVarcharColInRow(0);
  EXPECT_STREQ(str, "pg_catalog");
  free(str);

  table_entry_ptr = table_handle.GetTableEntry(txn_, "pg_class");
  EXPECT_NE(table_entry_ptr, nullptr);
  str = table_entry_ptr->GetVarcharColInRow(0);
  EXPECT_STREQ(str, "pg_catalog");
  free(str);

  // test if get correct tablespace
  table_entry_ptr = table_handle.GetTableEntry(txn_, "pg_database");
  EXPECT_NE(table_entry_ptr, nullptr);
  str = table_entry_ptr->GetVarcharColInRow(2);
  EXPECT_STREQ(str, "pg_global");
  free(str);

  table_entry_ptr = table_handle.GetTableEntry(txn_, "pg_tablespace");
  EXPECT_NE(table_entry_ptr, nullptr);
  str = table_entry_ptr->GetVarcharColInRow(2);
  EXPECT_STREQ(str, "pg_global");
  free(str);

  table_entry_ptr = table_handle.GetTableEntry(txn_, "pg_namespace");
  EXPECT_NE(table_entry_ptr, nullptr);
  str = table_entry_ptr->GetVarcharColInRow(2);
  EXPECT_STREQ(str, "pg_default");
  free(str);

  table_entry_ptr = table_handle.GetTableEntry(txn_, "pg_class");
  EXPECT_NE(table_entry_ptr, nullptr);
  str = table_entry_ptr->GetVarcharColInRow(2);
  EXPECT_STREQ(str, "pg_default");
  free(str);
}

// Tests for creating a table
// NOLINTNEXTLINE
TEST_F(TableHandleTests, CreateTest) {
  txn_ = txn_manager_->BeginTransaction();
  // terrier has db_oid_t DEFAULT_DATABASE_OID
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
  auto db_handle = catalog_->GetDatabaseHandle();
  auto table_handle = db_handle.GetNamespaceHandle(txn_, terrier_oid).GetTableHandle(txn_, "public");

  // define schema
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("id", type::TypeId::INTEGER, false, catalog::col_oid_t(catalog_->GetNextOid()));
  cols.emplace_back("name", type::TypeId::VARCHAR, false, catalog::col_oid_t(catalog_->GetNextOid()));
  catalog::Schema schema(cols);

  // create table
  table_handle.CreateTable(txn_, schema, "test_table");

  auto table_entry = table_handle.GetTableEntry(txn_, "test_table");
  EXPECT_NE(table_entry, nullptr);
  auto str = table_entry->GetVarcharColInRow(0);
  EXPECT_STREQ(str, "public");
  free(str);
  str = table_entry->GetVarcharColInRow(1);
  EXPECT_STREQ(str, "test_table");
  free(str);
  str = table_entry->GetVarcharColInRow(2);
  EXPECT_STREQ(str, "pg_default");
  free(str);
}
}  // namespace terrier
