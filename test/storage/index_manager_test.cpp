#include "storage/index/index_manager.h"
#include <string>
#include <vector>
#include "catalog/catalog_sql_table.h"
#include "catalog/namespace_handle.h"
#include "catalog/index_handle.h"
#include "storage/index/index_factory.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"

namespace terrier::storage::index {
struct IndexManagerTest : public TerrierTest {
  void SetUp() override {
    TerrierTest::SetUp();
    txn_manager_ = new transaction::TransactionManager(&buffer_pool_, true, LOGGING_DISABLED);
    index_manager_ = new IndexManager();
  }

  void TearDown() override {
    TerrierTest::TearDown();
    delete index_manager_;
    delete txn_manager_;
  }
  storage::RecordBufferSegmentPool buffer_pool_{100, 100};
  transaction::TransactionManager *txn_manager_;
  IndexManager *index_manager_;
};

// Check the basic functionality of create index concurrently
// NOLINTNEXTLINE
TEST_F(IndexManagerTest, CreateIndexConcurrentlyBasicTest) {
  auto txn0 = txn_manager_->BeginTransaction();
  auto catalog_ = new catalog::Catalog(txn_manager_, txn0);

  // terrier has db_oid_t DEFAULT_DATABASE_OID
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
  auto db_handle = catalog_->GetDatabaseHandle();
  auto ns_handle = db_handle.GetNamespaceHandle(txn0, terrier_oid);
  auto table_handle = ns_handle.GetTableHandle(txn0, "public");
  auto ns_oid = ns_handle.NameToOid(txn0, std::string("public"));

  // define schema
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("sex", type::TypeId::BOOLEAN, false, catalog::col_oid_t(catalog_->GetNextOid()));
  cols.emplace_back("id", type::TypeId::INTEGER, false, catalog::col_oid_t(catalog_->GetNextOid()));
  cols.emplace_back("name", type::TypeId::VARCHAR, 100, false, catalog::col_oid_t(catalog_->GetNextOid()));
  cols.emplace_back("address", type::TypeId::VARCHAR, 200, false, catalog::col_oid_t(catalog_->GetNextOid()));
  catalog::Schema schema(cols);

  // create table
  auto table = table_handle.CreateTable(txn0, schema, "test_table");
  auto table_oid = table_handle.NameToOid(txn0, "test_table");
  auto table_entry = table_handle.GetTableEntry(txn0, "test_table");
  EXPECT_NE(table_entry, nullptr);
  std::string_view str = type::TransientValuePeeker::PeekVarChar(table_entry->GetColInRow(0));
  EXPECT_EQ(str, "public");

  str = type::TransientValuePeeker::PeekVarChar(table_entry->GetColInRow(1));
  EXPECT_EQ(str, "test_table");

  str = type::TransientValuePeeker::PeekVarChar(table_entry->GetColInRow(2));
  EXPECT_EQ(str, "pg_default");

  // Insert a few rows into the table
  auto ptr = table_handle.GetTable(txn0, "test_table");
  EXPECT_EQ(ptr, table);

  for (int i = 0; i < 200; ++i) {
    std::vector<type::TransientValue> row;
    row.emplace_back(type::TransientValueFactory::GetBoolean(i % 2 == 0));
    row.emplace_back(type::TransientValueFactory::GetInteger(i));
    row.emplace_back(type::TransientValueFactory::GetVarChar(fmt::format("name_%d", i)));
    row.emplace_back(type::TransientValueFactory::GetVarChar(fmt::format("address_%d", i)));
    ptr->InsertRow(txn0, row);
  }

  // Commit the setting transaction
  txn_manager_->Commit(txn0, TestCallbacks::EmptyCallback, nullptr);

  // Set up index attributes and key attributes
  std::vector<std::string> index_attrs{"sex", "id", "name"};
  std::vector<std::string> key_attrs{"id", "name"};

  // Create the index
  auto index_oid = index_manager_->CreateConcurrently(terrier_oid, ns_oid, table_oid, parser::IndexType::BWTREE, false,
                                                      "test_index", index_attrs, key_attrs, txn_manager_, catalog_);
  EXPECT_GT(!index_oid, 0);

  // Test whether the catalog has the corresponding information
  auto txn1 = txn_manager_->BeginTransaction();
  auto index_handle = db_handle.GetIndexHandle(txn1, terrier_oid);
  auto index_entry = index_handle.GetIndexEntry(txn1, index_oid);
  EXPECT_NE(index_entry, nullptr);
  auto ret = index_entry->GetIntegerColumn("indexrelid");
  EXPECT_EQ(ret, !index_oid);
  EXPECT_EQ(index_entry->GetIntegerColumn("indrelid"), !table_oid);
  EXPECT_EQ(index_entry->GetBooleanColumn("indisready"), false);
  EXPECT_EQ(index_entry->GetBooleanColumn("indisvalid"), true);
  txn_manager_->Commit(txn1, TestCallbacks::EmptyCallback, nullptr);

  // Test whether index contains all entries inserted before
  auto txn2 = txn_manager_->BeginTransaction();
  Index *index = reinterpret_cast<Index *>(index_entry->GetBigIntColumn("indexptr"));
  // Create the projected row for index
  const IndexMetadata &metadata = index->GetIndexMetadata();
  const IndexKeySchema &index_key_schema = metadata.GetKeySchema();
  const auto &pr_initializer = metadata.GetProjectedRowInitializer();
  auto *key_buf_index = common::AllocationUtil::AllocateAligned(pr_initializer.ProjectedRowSize());
  ProjectedRow *index_key = pr_initializer.InitializeRow(key_buf_index);

  // Create the projected row for the sql table
  std::vector<catalog::col_oid_t> col_oids;
  col_oids.reserve(index_key_schema.size());
  for (const auto &it : index_key_schema) {
    col_oids.emplace_back(catalog::col_oid_t(!it.GetOid()));
  }
  auto init_and_map = table->GetSqlTable()->InitializerForProjectedRow(col_oids);
  auto *key_buf = common::AllocationUtil::AllocateAligned(init_and_map.first.ProjectedRowSize());
  ProjectedRow *key = init_and_map.first.InitializeRow(key_buf);

  // Record the col_id of each column
  std::vector<col_id_t> sql_table_cols;
  sql_table_cols.reserve(key->NumColumns());
  for (uint16_t i = 0; i < key->NumColumns(); ++i) {
    sql_table_cols.emplace_back(key->ColumnIds()[i]);
  }
  for (const auto &it : *table->GetSqlTable()) {
    if (table->GetSqlTable()->Select(txn2, it, key)) {
      for (uint16_t i = 0; i < key->NumColumns(); ++i) {
        key->ColumnIds()[i] = index_key->ColumnIds()[i];
      }
      std::vector<TupleSlot> tup_slots;
      index->ScanKey(*txn2, *key, &tup_slots);
      bool flag = false;
      for (const auto &tup_slot : tup_slots) {
        if (it == tup_slot) {
          flag = true;
        }
      }
      EXPECT_EQ(flag, true);
      for (uint16_t i = 0; i < key->NumColumns(); ++i) {
        key->ColumnIds()[i] = sql_table_cols[i];
      }
    }
  }
  txn_manager_->Commit(txn2, TestCallbacks::EmptyCallback, nullptr);

  delete table;
  delete catalog_;
  delete txn0;
  delete txn1;
  delete txn2;
  delete index;
  delete[] key_buf_index;
  delete[] key_buf;
}

// Check that throwed exceptions are properly handled
// NOLINTNEXTLINE
TEST_F(IndexManagerTest, CreateIndexConcurrentlyExceptionTest) {
  auto txn0 = txn_manager_->BeginTransaction();
  auto catalog_ = new catalog::Catalog(txn_manager_, txn0);

  // terrier has db_oid_t DEFAULT_DATABASE_OID
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
  auto db_handle = catalog_->GetDatabaseHandle();
  auto ns_handle = db_handle.GetNamespaceHandle(txn0, terrier_oid);
  auto table_handle = ns_handle.GetTableHandle(txn0, "public");
  auto ns_oid = ns_handle.NameToOid(txn0, std::string("public"));

  // define schema
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("sex", type::TypeId::BOOLEAN, false, catalog::col_oid_t(catalog_->GetNextOid()));
  cols.emplace_back("id", type::TypeId::INTEGER, false, catalog::col_oid_t(catalog_->GetNextOid()));
  cols.emplace_back("name", type::TypeId::VARCHAR, 100, false, catalog::col_oid_t(catalog_->GetNextOid()));
  cols.emplace_back("address", type::TypeId::VARCHAR, 200, false, catalog::col_oid_t(catalog_->GetNextOid()));
  catalog::Schema schema(cols);

  // create table
  auto table = table_handle.CreateTable(txn0, schema, "test_table");
  auto table_oid = table_handle.NameToOid(txn0, "test_table");
  auto table_entry = table_handle.GetTableEntry(txn0, "test_table");
  EXPECT_NE(table_entry, nullptr);
  std::string_view str = type::TransientValuePeeker::PeekVarChar(table_entry->GetColInRow(0));
  EXPECT_EQ(str, "public");

  str = type::TransientValuePeeker::PeekVarChar(table_entry->GetColInRow(1));
  EXPECT_EQ(str, "test_table");

  str = type::TransientValuePeeker::PeekVarChar(table_entry->GetColInRow(2));
  EXPECT_EQ(str, "pg_default");

  // Insert a few rows into the table
  auto ptr = table_handle.GetTable(txn0, "test_table");
  EXPECT_EQ(ptr, table);

  for (int i = 0; i < 200; ++i) {
    std::vector<type::TransientValue> row;
    row.emplace_back(type::TransientValueFactory::GetBoolean(i % 2 == 0));
    row.emplace_back(type::TransientValueFactory::GetInteger(i));
    row.emplace_back(type::TransientValueFactory::GetVarChar(fmt::format("name_%d", i)));
    row.emplace_back(type::TransientValueFactory::GetVarChar(fmt::format("address_%d", i)));
    ptr->InsertRow(txn0, row);
  }

  // Commit the setting transaction
  txn_manager_->Commit(txn0, TestCallbacks::EmptyCallback, nullptr);

  // Set up index attributes and key attributes
  std::vector<std::string> index_attrs{"sex", "id", "name"};
  std::vector<std::string> key_attrs{"id", "non-exist"};

  // Create the index
  auto index_oid = index_manager_->CreateConcurrently(terrier_oid, ns_oid, table_oid, parser::IndexType::BWTREE, false,
                                                      "test_index", index_attrs, key_attrs, txn_manager_, catalog_);
  EXPECT_EQ(!index_oid, 0);

  delete table;
  delete catalog_;
  delete txn0;
}

// Check the correctness of drop index
// NOLINTNEXTLINE
TEST_F(IndexManagerTest, DropIndexCorrectnessTest) {
  auto txn0 = txn_manager_->BeginTransaction();
  auto catalog_ = new catalog::Catalog(txn_manager_, txn0);

  // terrier has db_oid_t DEFAULT_DATABASE_OID
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
  auto db_handle = catalog_->GetDatabaseHandle();
  auto ns_handle = db_handle.GetNamespaceHandle(txn0, terrier_oid);
  auto table_handle = ns_handle.GetTableHandle(txn0, "public");
  auto ns_oid = ns_handle.NameToOid(txn0, std::string("public"));

  // define schema
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("sex", type::TypeId::BOOLEAN, false, catalog::col_oid_t(catalog_->GetNextOid()));
  cols.emplace_back("id", type::TypeId::INTEGER, false, catalog::col_oid_t(catalog_->GetNextOid()));
  cols.emplace_back("name", type::TypeId::VARCHAR, 100, false, catalog::col_oid_t(catalog_->GetNextOid()));
  cols.emplace_back("address", type::TypeId::VARCHAR, 200, false, catalog::col_oid_t(catalog_->GetNextOid()));
  catalog::Schema schema(cols);

  // create table
  auto table = table_handle.CreateTable(txn0, schema, "test_table");
  auto table_oid = table_handle.NameToOid(txn0, "test_table");
  auto table_entry = table_handle.GetTableEntry(txn0, "test_table");
  EXPECT_NE(table_entry, nullptr);
  std::string_view str = type::TransientValuePeeker::PeekVarChar(table_entry->GetColInRow(0));
  EXPECT_EQ(str, "public");

  str = type::TransientValuePeeker::PeekVarChar(table_entry->GetColInRow(1));
  EXPECT_EQ(str, "test_table");

  str = type::TransientValuePeeker::PeekVarChar(table_entry->GetColInRow(2));
  EXPECT_EQ(str, "pg_default");

  // Insert a few rows into the table
  auto ptr = table_handle.GetTable(txn0, "test_table");
  EXPECT_EQ(ptr, table);

  for (int i = 0; i < 200; ++i) {
    std::vector<type::TransientValue> row;
    row.emplace_back(type::TransientValueFactory::GetBoolean(i % 2 == 0));
    row.emplace_back(type::TransientValueFactory::GetInteger(i));
    row.emplace_back(type::TransientValueFactory::GetVarChar(fmt::format("name_%d", i)));
    row.emplace_back(type::TransientValueFactory::GetVarChar(fmt::format("address_%d", i)));
    ptr->InsertRow(txn0, row);
  }

  // Commit the setting transaction
  txn_manager_->Commit(txn0, TestCallbacks::EmptyCallback, nullptr);

  // Set up index attributes and key attributes
  std::vector<std::string> index_attrs{"sex", "id", "name"};
  std::vector<std::string> key_attrs{"id", "name"};

  // Create the index
  auto index_oid = index_manager_->CreateConcurrently(terrier_oid, ns_oid, table_oid, parser::IndexType::BWTREE, false,
                                                      "test_index", index_attrs, key_attrs, txn_manager_, catalog_);

  // Test whether the catalog has the corresponding information
  auto txn1 = txn_manager_->BeginTransaction();
  EXPECT_GT(!index_oid, 0);
  auto index_handle = db_handle.GetIndexHandle(txn1, terrier_oid);
  auto index_entry = index_handle.GetIndexEntry(txn1, index_oid);
  EXPECT_NE(index_entry, nullptr);
  auto ret = index_entry->GetIntegerColumn("indexrelid");
  EXPECT_EQ(ret, !index_oid);
  EXPECT_EQ(index_entry->GetIntegerColumn("indrelid"), !table_oid);
  EXPECT_EQ(index_entry->GetBooleanColumn("indisready"), false);
  EXPECT_EQ(index_entry->GetBooleanColumn("indisvalid"), true);
  txn_manager_->Commit(txn1, TestCallbacks::EmptyCallback, nullptr);

  // Drop the index and test the information does not exist
  index_manager_->Drop(terrier_oid, ns_oid, table_oid, index_oid, "test_index", txn_manager_, catalog_);
  auto txn2 = txn_manager_->BeginTransaction();
  index_entry = index_handle.GetIndexEntry(txn2, index_oid);
  EXPECT_EQ(index_entry, nullptr);
  txn_manager_->Commit(txn2, TestCallbacks::EmptyCallback, nullptr);

  delete table;
  delete catalog_;
  delete txn0;
  delete txn1;
  delete txn2;
}
}  // namespace terrier::storage::index
