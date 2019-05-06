#include "storage/index/index_manager.h"
#include <string>
#include <vector>
#include "catalog/catalog_sql_table.h"
#include "storage/index/index_factory.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"

namespace terrier::storage::index {
struct IndexManagerTest : public TerrierTest {
  void SetUp() override {
    TerrierTest::SetUp();
    txn_manager_ = new transaction::TransactionManager(&buffer_pool_, true, LOGGING_DISABLED);
    index_manager_ = new IndexManager();

    txn_ = txn_manager_->BeginTransaction();
    catalog_ = new catalog::Catalog(txn_manager_, txn_);
  }

  void TearDown() override {
    TerrierTest::TearDown();
    delete catalog_;  // delete catalog first
    delete txn_manager_;
    delete txn_;
  }

  catalog::Catalog *catalog_;
  storage::RecordBufferSegmentPool buffer_pool_{100, 100};

  transaction::TransactionContext *txn_;
  transaction::TransactionManager *txn_manager_;
  IndexManager *index_manager_;
};

// Check the correctness of drop index
// NOLINTNEXTLINE
TEST_F(IndexManagerTest, DropIndexCorrectnessTest) {
  // terrier has db_oid_t DEFAULT_DATABASE_OID
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
  auto db_handle = catalog_->GetDatabaseHandle();
  auto ns_handle = db_handle.GetNamespaceHandle(txn_, terrier_oid);
  auto table_handle = ns_handle.GetTableHandle(txn_, "public");
  auto ns_oid = ns_handle.NameToOid(txn_, std::string("public"));

  // define schema
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("sex", type::TypeId::BOOLEAN, false, catalog::col_oid_t(catalog_->GetNextOid()));
  cols.emplace_back("id", type::TypeId::INTEGER, false, catalog::col_oid_t(catalog_->GetNextOid()));
  cols.emplace_back("name", type::TypeId::VARCHAR, 100, false, catalog::col_oid_t(catalog_->GetNextOid()));
  cols.emplace_back("address", type::TypeId::VARCHAR, 200, false, catalog::col_oid_t(catalog_->GetNextOid()));
  catalog::Schema schema(cols);

  // create table
  auto table = table_handle.CreateTable(txn_, schema, "test_table");
  auto table_oid = table_handle.NameToOid(txn_, "test_table");
  auto table_entry = table_handle.GetTableEntry(txn_, "test_table");
  EXPECT_NE(table_entry, nullptr);
  std::string_view str = type::TransientValuePeeker::PeekVarChar(table_entry->GetColInRow(0));
  EXPECT_EQ(str, "public");

  str = type::TransientValuePeeker::PeekVarChar(table_entry->GetColInRow(1));
  EXPECT_EQ(str, "test_table");

  str = type::TransientValuePeeker::PeekVarChar(table_entry->GetColInRow(2));
  EXPECT_EQ(str, "pg_default");

  // Insert a few rows into the table
  auto ptr = table_handle.GetTable(txn_, "test_table");
  EXPECT_EQ(ptr, table);

  for (int i = 0; i < 200; ++i) {
    std::vector<type::TransientValue> row;
    std::ostringstream stringStream;
    stringStream << "name_" << i;
    std::string name_string = stringStream.str();
    stringStream << "address_" << i;
    std::string address_string = stringStream.str();
    row.emplace_back(type::TransientValueFactory::GetBoolean(i % 2 == 0));
    row.emplace_back(type::TransientValueFactory::GetInteger(i));
    row.emplace_back(type::TransientValueFactory::GetVarChar(name_string));
    row.emplace_back(type::TransientValueFactory::GetVarChar(address_string));
    ptr->InsertRow(txn_, row);
  }

  // Commit the setting transaction
  txn_manager_->Commit(txn_, TestCallbacks::EmptyCallback, nullptr);

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
  delete txn1;
  delete txn2;
}
}  // namespace terrier::storage::index
