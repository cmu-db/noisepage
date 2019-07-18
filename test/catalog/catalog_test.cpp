#include "catalog/catalog.h"
#include <algorithm>
#include <random>
#include <string>
#include <vector>
#include "catalog/catalog_accessor.h"
#include "catalog/catalog_defs.h"
#include "parser/expression/constant_value_expression.h"
#include "storage/garbage_collector.h"
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
    txn_manager_ = new transaction::TransactionManager(&buffer_pool_, true, LOGGING_DISABLED);
    gc_ = new storage::GarbageCollector(txn_manager_);

    // Build out the catalog and commit so that it is visible to other transactions
    auto txn = txn_manager_->BeginTransaction();
    catalog_ = new catalog::Catalog(txn_manager_, &block_store_);
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Run the GC to flush it down to a clean system
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
  }

  void TearDown() override {
    // Run the GC to clean up transactions
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();

    delete catalog_;  // need to delete catalog_first
    delete gc_;
    delete txn_manager_;

    TerrierTest::TearDown();
  }

  void VerifyCatalogTables(catalog::CatalogAccessor *accessor) {
    auto ns_oid = accessor->GetNamespaceOid("pg_catalog");
    EXPECT_NE(ns_oid, catalog::INVALID_NAMESPACE_OID);

    VerifyTablePresent(accessor, ns_oid, "pg_attribute");
    VerifyTablePresent(accessor, ns_oid, "pg_attrdef");
    VerifyTablePresent(accessor, ns_oid, "pg_class");
    VerifyTablePresent(accessor, ns_oid, "pg_namespace");
    VerifyTablePresent(accessor, ns_oid, "pg_type");
  }

  void VerifyTablePresent(catalog::CatalogAccessor *accessor, catalog::namespace_oid_t ns_oid,
                          const std::string &table_name) {
    auto table_oid = accessor->GetTableOid(ns_oid, table_name);
    EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  }

  void VerifyTableAbsent(catalog::CatalogAccessor *accessor, catalog::namespace_oid_t ns_oid,
                         const std::string &table_name) {
    auto table_oid = accessor->GetTableOid(ns_oid, table_name);
    EXPECT_EQ(table_oid, catalog::INVALID_TABLE_OID);
  }

  catalog::Catalog *catalog_;
  storage::RecordBufferSegmentPool buffer_pool_{100, 100};
  storage::BlockStore block_store_{100, 100};

  transaction::TransactionManager *txn_manager_;

  storage::GarbageCollector *gc_;
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
  VerifyCatalogTables(accessor);  // Check visibility to me
  auto tmp_oid = accessor->CreateDatabase("test_database");
  EXPECT_EQ(tmp_oid, catalog::INVALID_DATABASE_OID);  // Should cause a name conflict
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  gc_->PerformGarbageCollection();
  gc_->PerformGarbageCollection();

  // Get an accessor into the database and validate the catalog tables exist
  // then delete it and verify an invalid OID is now returned for the lookup
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(txn, db_oid);
  VerifyCatalogTables(accessor);  // Check visibility to me
  tmp_oid = accessor->GetDatabaseOid("test_database");
  EXPECT_TRUE(accessor->DropDatabase(tmp_oid));
  EXPECT_FALSE(accessor->DropDatabase(tmp_oid));  // Cannot drop a database twice
  tmp_oid = accessor->GetDatabaseOid("test_database");
  EXPECT_EQ(tmp_oid, catalog::INVALID_DATABASE_OID);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  gc_->PerformGarbageCollection();
  gc_->PerformGarbageCollection();
}

/*
 * Create and delete a namespace
 */
// NOLINTNEXTLINE
TEST_F(CatalogTests, NamespaceTest) {
  // Create a database and check that it's immediately visible
  auto txn = txn_manager_->BeginTransaction();
  auto db_oid = catalog_->CreateDatabase(txn, "test_database", true);
  EXPECT_NE(db_oid, catalog::INVALID_DATABASE_OID);
  auto accessor = catalog_->GetAccessor(txn, db_oid);
  auto ns_oid = accessor->CreateNamespace("test_namespace");
  EXPECT_NE(ns_oid, catalog::INVALID_NAMESPACE_OID);
  VerifyCatalogTables(accessor);  // Check visibility to me
  ns_oid = accessor->CreateNamespace("test_namespace");
  EXPECT_EQ(ns_oid, catalog::INVALID_NAMESPACE_OID);  // Should cause a name conflict
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  gc_->PerformGarbageCollection();
  gc_->PerformGarbageCollection();

  // Get an accessor into the database and validate the catalog tables exist
  // then delete it and verify an invalid OID is now returned for the lookup
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(txn, db_oid);
  VerifyCatalogTables(accessor);  // Check visibility to me
  ns_oid = accessor->GetNamespaceOid("test_namespace");
  EXPECT_TRUE(accessor->DropNamespace(ns_oid));
  ns_oid = accessor->GetNamespaceOid("test_namespace");
  EXPECT_EQ(ns_oid, catalog::INVALID_NAMESPACE_OID);
  EXPECT_FALSE(accessor->DropNamespace(ns_oid));
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  gc_->PerformGarbageCollection();
  gc_->PerformGarbageCollection();
}

/*
 * Create and delete a user table.
 */
// NOLINTNEXTLINE
TEST_F(CatalogTests, UserTableTest) {
  auto txn = txn_manager_->BeginTransaction();
  auto db_oid = catalog_->CreateDatabase(txn, "test_database", true);
  EXPECT_NE(db_oid, catalog::INVALID_DATABASE_OID);
  auto accessor = catalog_->GetAccessor(txn, db_oid);

  // Create the column definition (no OIDs)
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("id", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
  cols.emplace_back("user_col_1", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
  auto tmp_schema = catalog::Schema(cols);

  auto table_oid = accessor->CreateTable(accessor->GetDefaultNamespace(), "test_table", tmp_schema);
  EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  VerifyTablePresent(accessor, accessor->GetDefaultNamespace(), "test_table");
  // Check lookup via search path
  EXPECT_EQ(table_oid, accessor->GetTableOid("test_table"));
  EXPECT_EQ(accessor->GetTable(table_oid), nullptr);  // Check that allocation has not happened
  auto schema = accessor->GetSchema(table_oid);

  // Verify our columns exist
  EXPECT_NE(schema.GetColumn("id").GetOid(), catalog::INVALID_COLUMN_OID);
  EXPECT_NE(schema.GetColumn("user_col_1").GetOid(), catalog::INVALID_COLUMN_OID);

  // Verify we can instantiate a storage object with the generated schema
  auto table = new storage::SqlTable(&block_store_, schema);

  // TODO(John): The next call should not transfer ownership of the SqlTable to
  // the catalog.  However, the current backend does this.  This test will leak
  // once this is corrected unless the delete call at the end is uncommented.
  accessor->SetTablePointer(table_oid, table);
  EXPECT_EQ(table, accessor->GetTable(table_oid));
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  gc_->PerformGarbageCollection();
  gc_->PerformGarbageCollection();

  // Get an accessor into the database and validate the catalog tables exist
  // then delete it and verify an invalid OID is now returned for the lookup
  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(txn, db_oid);
  table_oid = accessor->GetTableOid("test_table");
  EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  EXPECT_TRUE(accessor->DropTable(table_oid));
  EXPECT_FALSE(accessor->DropTable(table_oid));
  table_oid = accessor->GetTableOid("test_table");
  EXPECT_EQ(table_oid, catalog::INVALID_TABLE_OID);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  gc_->PerformGarbageCollection();
  gc_->PerformGarbageCollection();
}

}  // namespace terrier
