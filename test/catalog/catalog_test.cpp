#include "catalog/catalog.h"
#include <algorithm>
#include <random>
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog_accessor.h"
#include "catalog/catalog_defs.h"
#include "catalog/postgres/pg_namespace.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "storage/garbage_collector.h"
#include "storage/index/index_builder.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_manager.h"
#include "transaction/transaction_util.h"
#include "type/transient_value_factory.h"
#include "util/test_harness.h"

namespace terrier {

struct CatalogTests : public TerrierTest {
  void SetUp() override {
    TerrierTest::SetUp();

    // Initialize the transaction manager and GC
    timestamp_manager_ = new transaction::TimestampManager;
    deferred_action_manager_ = new transaction::DeferredActionManager(timestamp_manager_);
    txn_manager_ = new transaction::TransactionManager(timestamp_manager_, deferred_action_manager_, &buffer_pool_,
                                                       true, DISABLED);
    gc_ = new storage::GarbageCollector(timestamp_manager_, deferred_action_manager_, txn_manager_, nullptr);

    // Build out the catalog and commit so that it is visible to other transactions
    catalog_ = new catalog::Catalog(txn_manager_, &block_store_);

    auto txn = txn_manager_->BeginTransaction();
    db_ = catalog_->CreateDatabase(txn, "terrier", true);
    EXPECT_NE(db_, catalog::INVALID_DATABASE_OID);
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Run the GC to flush it down to a clean system
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
  }

  void TearDown() override {
    catalog_->TearDown();
    // Run the GC to clean up transactions
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();

    delete catalog_;  // need to delete catalog_first
    delete gc_;
    delete txn_manager_;
    delete deferred_action_manager_;
    delete timestamp_manager_;

    TerrierTest::TearDown();
  }

  void VerifyCatalogTables(const catalog::CatalogAccessor &accessor) {
    auto ns_oid = accessor.GetNamespaceOid("pg_catalog");
    EXPECT_NE(ns_oid, catalog::INVALID_NAMESPACE_OID);
    EXPECT_EQ(ns_oid, catalog::postgres::NAMESPACE_CATALOG_NAMESPACE_OID);

    VerifyTablePresent(accessor, ns_oid, "pg_attribute");
    VerifyTablePresent(accessor, ns_oid, "pg_class");
    VerifyTablePresent(accessor, ns_oid, "pg_constraint");
    VerifyTablePresent(accessor, ns_oid, "pg_index");
    VerifyTablePresent(accessor, ns_oid, "pg_namespace");
    VerifyTablePresent(accessor, ns_oid, "pg_type");
  }

  void VerifyTablePresent(const catalog::CatalogAccessor &accessor, catalog::namespace_oid_t ns_oid,
                          const std::string &table_name) {
    auto table_oid = accessor.GetTableOid(ns_oid, table_name);
    EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  }

  void VerifyTableAbsent(const catalog::CatalogAccessor &accessor, catalog::namespace_oid_t ns_oid,
                         const std::string &table_name) {
    auto table_oid = accessor.GetTableOid(ns_oid, table_name);
    EXPECT_EQ(table_oid, catalog::INVALID_TABLE_OID);
  }

  catalog::Catalog *catalog_;
  storage::RecordBufferSegmentPool buffer_pool_{100, 100};
  storage::BlockStore block_store_{100, 100};
  transaction::TimestampManager *timestamp_manager_;
  transaction::DeferredActionManager *deferred_action_manager_;
  transaction::TransactionManager *txn_manager_;

  storage::GarbageCollector *gc_;
  catalog::db_oid_t db_;
};

/*
 * Create and delete a database
 */
// NOLINTNEXTLINE
TEST_F(CatalogTests, DatabaseTest) {
  // Create a database and check that it's immediately visible
  auto txn = txn_manager_->BeginTransaction();
  auto db_oid = catalog_->CreateDatabase(txn, "test_database", true);
  EXPECT_NE(db_oid, catalog::INVALID_DATABASE_OID);
  auto accessor = catalog_->GetAccessor(txn, db_oid);
  EXPECT_NE(accessor, nullptr);
  VerifyCatalogTables(*accessor);  // Check visibility to me
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Cannot add a database twice
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(txn, db_oid);
  auto tmp_oid = accessor->CreateDatabase("test_database");
  EXPECT_EQ(tmp_oid, catalog::INVALID_DATABASE_OID);  // Should cause a name conflict
  txn_manager_->Abort(txn);

  // Get an accessor into the database and validate the catalog tables exist
  // then delete it and verify an invalid OID is now returned for the lookup
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(txn, db_oid);
  EXPECT_NE(accessor, nullptr);
  VerifyCatalogTables(*accessor);  // Check visibility to me
  tmp_oid = accessor->GetDatabaseOid("test_database");
  EXPECT_TRUE(accessor->DropDatabase(tmp_oid));
  tmp_oid = accessor->GetDatabaseOid("test_database");
  EXPECT_EQ(tmp_oid, catalog::INVALID_DATABASE_OID);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Cannot get an accessor to a non-existent database
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(txn, db_oid);
  EXPECT_EQ(accessor, nullptr);
  txn_manager_->Abort(txn);
}

/*
 * Create and delete a namespace
 */
// NOLINTNEXTLINE
TEST_F(CatalogTests, NamespaceTest) {
  // Create a database and check that it's immediately visible
  auto txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(txn, db_);
  EXPECT_NE(accessor, nullptr);
  auto ns_oid = accessor->CreateNamespace("test_namespace");
  EXPECT_NE(ns_oid, catalog::INVALID_NAMESPACE_OID);
  VerifyCatalogTables(*accessor);  // Check visibility to me
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(txn, db_);
  ns_oid = accessor->CreateNamespace("test_namespace");
  EXPECT_EQ(ns_oid, catalog::INVALID_NAMESPACE_OID);  // Should cause a name conflict
  txn_manager_->Abort(txn);

  // Get an accessor into the database and validate the catalog tables exist
  // then delete it and verify an invalid OID is now returned for the lookup
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(txn, db_);
  EXPECT_NE(accessor, nullptr);
  VerifyCatalogTables(*accessor);  // Check visibility to me
  ns_oid = accessor->GetNamespaceOid("test_namespace");
  EXPECT_TRUE(accessor->DropNamespace(ns_oid));
  ns_oid = accessor->GetNamespaceOid("test_namespace");
  EXPECT_EQ(ns_oid, catalog::INVALID_NAMESPACE_OID);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(txn, db_);
  EXPECT_FALSE(accessor->DropNamespace(ns_oid));
  txn_manager_->Abort(txn);
}

/*
 * Create and delete a user table.
 */
// NOLINTNEXTLINE
TEST_F(CatalogTests, UserTableTest) {
  auto txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(txn, db_);

  // Create the column definition (no OIDs)
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("id", type::TypeId::INTEGER, false,
                    parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
  cols.emplace_back("user_col_1", type::TypeId::INTEGER, false,
                    parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
  auto tmp_schema = catalog::Schema(cols);

  auto table_oid = accessor->CreateTable(accessor->GetDefaultNamespace(), "test_table", tmp_schema);
  EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  VerifyTablePresent(*accessor, accessor->GetDefaultNamespace(), "test_table");
  // Check lookup via search path
  EXPECT_EQ(table_oid, accessor->GetTableOid("test_table"));
  EXPECT_EQ(accessor->GetTable(table_oid), nullptr);  // Check that allocation has not happened
  auto schema = accessor->GetSchema(table_oid);

  // Verify our columns exist
  EXPECT_NE(schema.GetColumn("id").Oid(), catalog::INVALID_COLUMN_OID);
  EXPECT_NE(schema.GetColumn("user_col_1").Oid(), catalog::INVALID_COLUMN_OID);

  // Verify we can instantiate a storage object with the generated schema
  auto table = new storage::SqlTable(&block_store_, schema);

  EXPECT_TRUE(accessor->SetTablePointer(table_oid, table));
  EXPECT_EQ(common::ManagedPointer(table), accessor->GetTable(table_oid));
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Get an accessor into the database and validate the catalog tables exist
  // then delete it and verify an invalid OID is now returned for the lookup
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(txn, db_);
  EXPECT_NE(accessor, nullptr);

  VerifyTablePresent(*accessor, accessor->GetDefaultNamespace(), "test_table");
  EXPECT_TRUE(accessor->DropTable(table_oid));
  VerifyTableAbsent(*accessor, accessor->GetDefaultNamespace(), "test_table");
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

/*
 *
 */
// NOLINTNEXTLINE
TEST_F(CatalogTests, UserIndexTest) {
  auto txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(txn, db_);

  // Create the column definition (no OIDs)
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("id", type::TypeId::INTEGER, false,
                    parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
  cols.emplace_back("user_col_1", type::TypeId::INTEGER, false,
                    parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
  auto tmp_schema = catalog::Schema(cols);

  auto table_oid = accessor->CreateTable(accessor->GetDefaultNamespace(), "test_table", tmp_schema);
  auto schema = accessor->GetSchema(table_oid);
  auto table = new storage::SqlTable(&block_store_, schema);

  EXPECT_TRUE(accessor->SetTablePointer(table_oid, table));

  // Create the index
  std::vector<catalog::IndexSchema::Column> key_cols{catalog::IndexSchema::Column{
      "id", type::TypeId::INTEGER, false, parser::ColumnValueExpression(db_, table_oid, schema.GetColumn("id").Oid())}};
  auto index_schema = catalog::IndexSchema(key_cols, storage::index::IndexType::BWTREE, true, true, false, true);
  auto idx_oid = accessor->CreateIndex(accessor->GetDefaultNamespace(), table_oid,
                                       "test_table_index_mabobberwithareallylongnamethatstillneedsmore", index_schema);
  EXPECT_NE(idx_oid, catalog::INVALID_INDEX_OID);
  auto true_schema = accessor->GetIndexSchema(idx_oid);

  storage::index::IndexBuilder index_builder;
  index_builder.SetKeySchema(true_schema);
  auto index = index_builder.Build();

  EXPECT_TRUE(accessor->SetIndexPointer(idx_oid, index));
  EXPECT_EQ(common::ManagedPointer(index), accessor->GetIndex(idx_oid));
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Get an accessor into the database and validate the catalog tables exist
  // then delete it and verify an invalid OID is now returned for the lookup
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(txn, db_);
  EXPECT_NE(accessor, nullptr);
  idx_oid = accessor->GetIndexOid("test_table_index_mabobberwithareallylongnamethatstillneedsmore");
  EXPECT_NE(idx_oid, catalog::INVALID_INDEX_OID);
  EXPECT_TRUE(accessor->DropIndex(idx_oid));
  idx_oid = accessor->GetIndexOid("test_table_index_mabobberwithareallylongnamethatstillneedsmore");
  EXPECT_EQ(idx_oid, catalog::INVALID_INDEX_OID);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

/*
 * Check behavior of search path
 */
// NOLINTNEXTLINE
TEST_F(CatalogTests, UserSearchPathTest) {
  // Create a database and check that it's immediately visible
  auto txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(txn, db_);
  auto public_ns_oid = accessor->GetNamespaceOid("public");
  EXPECT_NE(public_ns_oid, catalog::INVALID_NAMESPACE_OID);
  EXPECT_EQ(public_ns_oid, catalog::postgres::NAMESPACE_DEFAULT_NAMESPACE_OID);
  auto test_ns_oid = accessor->CreateNamespace("test");
  EXPECT_NE(test_ns_oid, catalog::INVALID_NAMESPACE_OID);
  VerifyCatalogTables(*accessor);  // Check visibility to me

  // Create the column definition (no OIDs)
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("id", type::TypeId::INTEGER, false,
                    parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
  cols.emplace_back("user_col_1", type::TypeId::INTEGER, false,
                    parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
  auto tmp_schema = catalog::Schema(cols);

  // Insert a table into "public"
  auto public_table_oid = accessor->CreateTable(public_ns_oid, "test_table", tmp_schema);
  EXPECT_NE(public_table_oid, catalog::INVALID_TABLE_OID);
  auto schema = accessor->GetSchema(public_table_oid);
  auto table = new storage::SqlTable(&block_store_, schema);
  EXPECT_TRUE(accessor->SetTablePointer(public_table_oid, table));

  // Insert a table into "test"
  auto test_table_oid = accessor->CreateTable(test_ns_oid, "test_table", tmp_schema);
  EXPECT_NE(test_table_oid, catalog::INVALID_TABLE_OID);
  schema = accessor->GetSchema(test_table_oid);
  table = new storage::SqlTable(&block_store_, schema);
  EXPECT_TRUE(accessor->SetTablePointer(test_table_oid, table));

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Check that it matches the table in the first namespace in path
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(txn, db_);

  accessor->SetSearchPath({test_ns_oid, public_ns_oid});
  EXPECT_EQ(accessor->GetTableOid("test_table"), test_table_oid);

  accessor->SetSearchPath({public_ns_oid, test_ns_oid});
  EXPECT_EQ(accessor->GetTableOid("test_table"), public_table_oid);

  auto table_oid = accessor->CreateTable(test_ns_oid, "test_table", tmp_schema);
  EXPECT_EQ(table_oid, catalog::INVALID_TABLE_OID);
  table_oid = accessor->CreateTable(test_ns_oid, "test_table", tmp_schema);
  EXPECT_EQ(table_oid, catalog::INVALID_TABLE_OID);
  txn_manager_->Abort(txn);

  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(txn, db_);

  accessor->DropTable(test_table_oid);

  accessor->SetSearchPath({test_ns_oid, public_ns_oid});
  EXPECT_EQ(accessor->GetTableOid("test_table"), public_table_oid);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

/*
 * Checks specifically whether the implicit searching of pg_catalog works correctly
 */
// NOLINTNEXTLINE
TEST_F(CatalogTests, CatalogSearchPathTest) {
  auto txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(txn, db_);
  EXPECT_EQ(accessor->GetTableOid("pg_namespace"), catalog::postgres::NAMESPACE_TABLE_OID);

  // Create the column definition (no OIDs)
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("id", type::TypeId::INTEGER, false,
                    parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
  cols.emplace_back("user_col_1", type::TypeId::INTEGER, false,
                    parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
  auto tmp_schema = catalog::Schema(cols);

  // Check whether name conflict is inserted into the proper default (first in search path) and masked by implicit
  // addition of 'pg_catalog' at start of search path
  auto user_table_oid = accessor->CreateTable(accessor->GetDefaultNamespace(), "pg_namespace", tmp_schema);
  EXPECT_EQ(accessor->GetTableOid(catalog::postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, "pg_namespace"), user_table_oid);
  EXPECT_EQ(accessor->GetTableOid("pg_namespace"), catalog::postgres::NAMESPACE_TABLE_OID);

  // Explicitly set 'pg_catalog' as second in the search path and check proper searching
  accessor->SetSearchPath(
      {catalog::postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, catalog::postgres::NAMESPACE_CATALOG_NAMESPACE_OID});
  EXPECT_EQ(accessor->GetTableOid("pg_namespace"), user_table_oid);
  EXPECT_EQ(accessor->GetTableOid(catalog::postgres::NAMESPACE_CATALOG_NAMESPACE_OID, "pg_namespace"),
            catalog::postgres::NAMESPACE_TABLE_OID);

  // Return to implicit declaration to ensure logic works correctly
  accessor->SetSearchPath({catalog::postgres::NAMESPACE_DEFAULT_NAMESPACE_OID});
  EXPECT_EQ(accessor->GetTableOid(catalog::postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, "pg_namespace"), user_table_oid);
  EXPECT_EQ(accessor->GetTableOid("pg_namespace"), catalog::postgres::NAMESPACE_TABLE_OID);

  // Close out
  txn_manager_->Abort(txn);
}

/*
 * Check that the normalize function in CatalogAccessor behaves correctly
 */
// NOLINTNEXTLINE
TEST_F(CatalogTests, NameNormalizationTest) {
  auto txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(txn, db_);
  auto ns_oid = accessor->CreateNamespace("TeSt_NaMeSpAcE");
  EXPECT_NE(ns_oid, catalog::INVALID_NAMESPACE_OID);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(txn, db_);
  EXPECT_EQ(catalog::INVALID_NAMESPACE_OID, accessor->CreateNamespace("TEST_NAMESPACE"));  // should conflict
  EXPECT_EQ(ns_oid, accessor->GetNamespaceOid("TEST_NAMESPACE"));                          // Should succeed
  auto dbc = catalog_->GetDatabaseCatalog(txn, db_);
  EXPECT_EQ(ns_oid, dbc->GetNamespaceOid(txn, "test_namespace"));  // Should match (normalized form)
  EXPECT_EQ(catalog::INVALID_NAMESPACE_OID, dbc->GetNamespaceOid(txn, "TeSt_NaMeSpAcE"));  // Not normalized
  txn_manager_->Abort(txn);
}

// NOLINTNEXTLINE
TEST_F(CatalogTests, GetIndexesTest) {
  auto txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(txn, db_);

  // Create the column definition (no OIDs)
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("id", type::TypeId::INTEGER, false,
                    parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
  auto tmp_schema = catalog::Schema(cols);

  auto table_oid = accessor->CreateTable(accessor->GetDefaultNamespace(), "test_table", tmp_schema);
  auto schema = accessor->GetSchema(table_oid);
  auto table = new storage::SqlTable(&block_store_, schema);
  EXPECT_TRUE(accessor->SetTablePointer(table_oid, table));

  // Create the index
  std::vector<catalog::IndexSchema::Column> key_cols{catalog::IndexSchema::Column{
      "id", type::TypeId::INTEGER, false, parser::ColumnValueExpression(db_, table_oid, schema.GetColumn("id").Oid())}};
  auto index_schema = catalog::IndexSchema(key_cols, storage::index::IndexType::BWTREE, true, true, false, true);
  auto idx_oid = accessor->CreateIndex(accessor->GetDefaultNamespace(), table_oid, "test_table_idx", index_schema);
  EXPECT_NE(idx_oid, catalog::INVALID_INDEX_OID);
  auto true_schema = accessor->GetIndexSchema(idx_oid);

  storage::index::IndexBuilder index_builder;
  index_builder.SetKeySchema(true_schema);
  auto index = index_builder.Build();

  EXPECT_TRUE(accessor->SetIndexPointer(idx_oid, index));
  EXPECT_EQ(common::ManagedPointer(index), accessor->GetIndex(idx_oid));
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Get an accessor into the database
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(txn, db_);
  EXPECT_NE(accessor, nullptr);

  // Check that GetIndexes returns the indexes
  auto idx_oids = accessor->GetIndexOids(table_oid);
  EXPECT_EQ(idx_oids.size(), 1);
  EXPECT_EQ(idx_oids[0], idx_oid);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// NOLINTNEXTLINE
TEST_F(CatalogTests, GetIndexObjectsTest) {
  constexpr auto num_indexes = 3;
  auto txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(txn, db_);

  // Create the column definition (no OIDs)
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("id", type::TypeId::INTEGER, false,
                    parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
  auto tmp_schema = catalog::Schema(cols);

  auto table_oid = accessor->CreateTable(accessor->GetDefaultNamespace(), "test_table", tmp_schema);
  auto schema = accessor->GetSchema(table_oid);
  auto table = new storage::SqlTable(&block_store_, schema);
  EXPECT_TRUE(accessor->SetTablePointer(table_oid, table));

  // Create the a couple of index
  std::vector<catalog::index_oid_t> index_oids;
  for (auto i = 0; i < num_indexes; i++) {
    std::vector<catalog::IndexSchema::Column> key_cols{
        catalog::IndexSchema::Column{"id", type::TypeId::INTEGER, false,
                                     parser::ColumnValueExpression(db_, table_oid, schema.GetColumn("id").Oid())}};
    auto index_schema = catalog::IndexSchema(key_cols, storage::index::IndexType::BWTREE, true, true, false, true);
    auto idx_oid = accessor->CreateIndex(accessor->GetDefaultNamespace(), table_oid,
                                         "test_table_idx" + std::to_string(i), index_schema);
    EXPECT_NE(idx_oid, catalog::INVALID_INDEX_OID);
    index_oids.push_back(idx_oid);
    const auto &true_schema = accessor->GetIndexSchema(idx_oid);

    storage::index::IndexBuilder index_builder;
    index_builder.SetKeySchema(true_schema);
    auto index = index_builder.Build();

    EXPECT_TRUE(accessor->SetIndexPointer(idx_oid, index));
    EXPECT_EQ(common::ManagedPointer(index), accessor->GetIndex(idx_oid));
  }
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Get an accessor into the database
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(txn, db_);
  EXPECT_NE(accessor, nullptr);

  // Check that GetIndexes returns the indexes correct number of indexes
  auto idx_oids = accessor->GetIndexOids(table_oid);
  EXPECT_EQ(num_indexes, idx_oids.size());

  // Fetch all objects with a single call, check that sets are equal
  auto index_objects = accessor->GetIndexes(table_oid);
  EXPECT_EQ(num_indexes, index_objects.size());
  for (const auto &object_pair : index_objects) {
    EXPECT_TRUE(object_pair.first);
    EXPECT_EQ(1, object_pair.second.GetColumns().size());
    EXPECT_EQ("id", object_pair.second.GetColumn(0).Name());
  }

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

}  // namespace terrier
