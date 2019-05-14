#include <algorithm>
#include <random>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/namespace_handle.h"
#include "catalog/schema.h"
#include "common/strong_typedef.h"
#include "transaction/transaction_manager.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"
namespace terrier {

// for tables in the test
#define MAX_VARCHAR_SIZE 1024

struct TableHandleTests : public TerrierTest {
  void SetUp() override {
    TerrierTest::SetUp();
    txn_manager_ = new transaction::TransactionManager(&buffer_pool_, true, LOGGING_DISABLED);

    txn_ = txn_manager_->BeginTransaction();
    catalog_ = new catalog::Catalog(txn_manager_, txn_);
  }

  void TearDown() override {
    txn_manager_->Commit(txn_, TestCallbacks::EmptyCallback, nullptr);

    TerrierTest::TearDown();
    delete catalog_;  // delete catalog first
    delete txn_manager_;
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
  // terrier has db_oid_t DEFAULT_DATABASE_OID
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
  auto db_handle = catalog_->GetDatabaseHandle();
  auto table_handle = db_handle.GetNamespaceTable(txn_, terrier_oid).GetTableHandle(txn_, "pg_catalog");

  // test if get correct tablename
  auto table_entry_ptr = table_handle.GetTableEntry(txn_, "pg_database");
  EXPECT_NE(table_entry_ptr, nullptr);
  std::string_view str_view = type::TransientValuePeeker::PeekVarChar(table_entry_ptr->GetColInRow(1));
  EXPECT_EQ(str_view, "pg_database");

  table_entry_ptr = table_handle.GetTableEntry(txn_, "pg_tablespace");
  EXPECT_NE(table_entry_ptr, nullptr);
  str_view = type::TransientValuePeeker::PeekVarChar(table_entry_ptr->GetColInRow(1));
  EXPECT_EQ(str_view, "pg_tablespace");

  table_entry_ptr = table_handle.GetTableEntry(txn_, "pg_namespace");
  EXPECT_NE(table_entry_ptr, nullptr);
  str_view = type::TransientValuePeeker::PeekVarChar(table_entry_ptr->GetColInRow(1));
  EXPECT_EQ(str_view, "pg_namespace");

  table_entry_ptr = table_handle.GetTableEntry(txn_, "pg_class");
  EXPECT_NE(table_entry_ptr, nullptr);
  str_view = type::TransientValuePeeker::PeekVarChar(table_entry_ptr->GetColInRow(1));
  EXPECT_EQ(str_view, "pg_class");

  // test if get correct schemaname (namespace)
  table_entry_ptr = table_handle.GetTableEntry(txn_, "pg_database");
  EXPECT_NE(table_entry_ptr, nullptr);
  str_view = type::TransientValuePeeker::PeekVarChar(table_entry_ptr->GetColInRow(0));
  EXPECT_EQ(str_view, "pg_catalog");

  table_entry_ptr = table_handle.GetTableEntry(txn_, "pg_tablespace");
  EXPECT_NE(table_entry_ptr, nullptr);
  str_view = type::TransientValuePeeker::PeekVarChar(table_entry_ptr->GetColInRow(0));
  EXPECT_EQ(str_view, "pg_catalog");

  table_entry_ptr = table_handle.GetTableEntry(txn_, "pg_namespace");
  EXPECT_NE(table_entry_ptr, nullptr);
  str_view = type::TransientValuePeeker::PeekVarChar(table_entry_ptr->GetColInRow(0));
  EXPECT_EQ(str_view, "pg_catalog");

  table_entry_ptr = table_handle.GetTableEntry(txn_, "pg_class");
  EXPECT_NE(table_entry_ptr, nullptr);
  str_view = type::TransientValuePeeker::PeekVarChar(table_entry_ptr->GetColInRow(0));
  EXPECT_EQ(str_view, "pg_catalog");

  // test if get correct tablespace
  table_entry_ptr = table_handle.GetTableEntry(txn_, "pg_database");
  EXPECT_NE(table_entry_ptr, nullptr);
  str_view = type::TransientValuePeeker::PeekVarChar(table_entry_ptr->GetColInRow(2));
  EXPECT_EQ(str_view, "pg_global");

  table_entry_ptr = table_handle.GetTableEntry(txn_, "pg_tablespace");
  EXPECT_NE(table_entry_ptr, nullptr);
  str_view = type::TransientValuePeeker::PeekVarChar(table_entry_ptr->GetColInRow(2));
  EXPECT_EQ(str_view, "pg_global");

  table_entry_ptr = table_handle.GetTableEntry(txn_, "pg_namespace");
  EXPECT_NE(table_entry_ptr, nullptr);
  str_view = type::TransientValuePeeker::PeekVarChar(table_entry_ptr->GetColInRow(2));
  EXPECT_EQ(str_view, "pg_default");

  table_entry_ptr = table_handle.GetTableEntry(txn_, "pg_class");
  EXPECT_NE(table_entry_ptr, nullptr);
  str_view = type::TransientValuePeeker::PeekVarChar(table_entry_ptr->GetColInRow(2));
  EXPECT_EQ(str_view, "pg_default");
}

// Tests for creating a table
// NOLINTNEXTLINE
TEST_F(TableHandleTests, CreateTest) {
  // terrier has db_oid_t DEFAULT_DATABASE_OID
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
  auto db_handle = catalog_->GetDatabaseHandle();
  auto table_handle = db_handle.GetNamespaceTable(txn_, terrier_oid).GetTableHandle(txn_, "public");

  // define schema
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("id", type::TypeId::INTEGER, false, catalog::col_oid_t(catalog_->GetNextOid()));
  cols.emplace_back("name", type::TypeId::VARCHAR, MAX_VARCHAR_SIZE, false, catalog::col_oid_t(catalog_->GetNextOid()));
  catalog::Schema schema(cols);

  // create table
  auto table = table_handle.CreateTable(txn_, schema, "test_table");
  auto table_entry = table_handle.GetTableEntry(txn_, "test_table");
  EXPECT_NE(table_entry, nullptr);
  std::string_view str = type::TransientValuePeeker::PeekVarChar(table_entry->GetColInRow(0));
  EXPECT_EQ(str, "public");

  str = type::TransientValuePeeker::PeekVarChar(table_entry->GetColInRow(1));
  EXPECT_EQ(str, "test_table");

  str = type::TransientValuePeeker::PeekVarChar(table_entry->GetColInRow(2));
  EXPECT_EQ(str, "pg_default");

  // Insert a row into the table
  auto ptr = table_handle.GetTable(txn_, "test_table");
  EXPECT_EQ(ptr, table);

  std::vector<type::TransientValue> row;
  row.emplace_back(type::TransientValueFactory::GetInteger(123));
  row.emplace_back(type::TransientValueFactory::GetVarChar("test_name"));
  ptr->InsertRow(txn_, row);

  // Read row from the table
  std::vector<type::TransientValue> search_vec;
  search_vec.emplace_back(type::TransientValueFactory::GetInteger(123));
  row.clear();
  row = ptr->FindRow(txn_, search_vec);
  EXPECT_EQ(type::TransientValuePeeker::PeekInteger(row[0]), 123);
  EXPECT_EQ(type::TransientValuePeeker::PeekVarChar(row[1]), "test_name");

  delete table;
}
}  // namespace terrier
