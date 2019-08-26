#include <string>
#include <unordered_map>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/postgres/pg_namespace.h"
#include "gtest/gtest.h"
#include "main/db_main.h"
#include "storage/garbage_collector_thread.h"
#include "storage/index/index_builder.h"
#include "storage/recovery/disk_log_provider.h"
#include "storage/recovery/recovery_manager.h"
#include "storage/sql_table.h"
#include "storage/write_ahead_log/log_manager.h"
#include "transaction/transaction_manager.h"
#include "util/catalog_test_util.h"
#include "util/sql_table_test_util.h"
#include "util/storage_test_util.h"
#include "util/test_harness.h"

#define LOG_FILE_NAME "./test.log"

namespace terrier::storage {
class RecoveryTests : public TerrierTest {
 protected:
  // Settings for log manager
  const uint64_t num_log_buffers_ = 100;
  const std::chrono::milliseconds log_serialization_interval_{10};
  const std::chrono::milliseconds log_persist_interval_{20};
  const uint64_t log_persist_threshold_ = (1 << 20);  // 1MB

  std::default_random_engine generator_;
  storage::RecordBufferSegmentPool pool_{2000, 100};
  storage::BlockStore block_store_{100, 100};

  storage::GarbageCollector *gc_;
  common::DedicatedThreadRegistry thread_registry_;

  void SetUp() override {
    // Unlink log file incase one exists from previous test iteration
    unlink(LOG_FILE_NAME);
    TerrierTest::SetUp();
  }

  void TearDown() override {
    // Delete log file
    unlink(LOG_FILE_NAME);
    TerrierTest::TearDown();
  }

  catalog::IndexSchema DummyIndexSchema() {
    std::vector<catalog::IndexSchema::Column> keycols;
    keycols.emplace_back(
        "", type::TypeId::INTEGER, false,
        parser::ColumnValueExpression(catalog::db_oid_t(0), catalog::table_oid_t(0), catalog::col_oid_t(1)));
    StorageTestUtil::ForceOid(&(keycols[0]), catalog::indexkeycol_oid_t(1));
    return catalog::IndexSchema(keycols, true, true, false, true);
  }

  void GarbageCollectCatalogObject(catalog::Catalog *catalog, transaction::TransactionManager *txn_manager) {
    // Perform GC 3 times to fully clean up catalog
    catalog->TearDown();
    gc_ = new storage::GarbageCollector(txn_manager, nullptr);
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
    delete gc_;
  }

  void RunTest(const LargeSqlTableTestConfiguration &config) {
    // Initialize table and run workload with logging enabled
    LogManager log_manager(LOG_FILE_NAME, num_log_buffers_, log_serialization_interval_, log_persist_interval_,
                           log_persist_threshold_, &pool_, common::ManagedPointer(&thread_registry_));
    log_manager.Start();
    auto *tested = new LargeSqlTableTestObject(config, &block_store_, &pool_, &generator_, &log_manager);
    tested->SimulateOltp(100, 4);

    // Simulate the system "shutting down". Guarantee persist of log records
    log_manager.PersistAndStop();

    /* We now "boot up" up the system and start recovery */

    // Start a transaction manager with logging disabled, we don't want to log the log replaying
    transaction::TransactionManager recovery_txn_manager{&pool_, true, LOGGING_DISABLED};

    // Create catalog for recovery
    catalog::Catalog recovered_catalog(&recovery_txn_manager, &block_store_);

    // Instantiate recovery manager, and recover the tables.
    DiskLogProvider log_provider(LOG_FILE_NAME);
    RecoveryManager recovery_manager(&log_provider, common::ManagedPointer(&recovered_catalog), &recovery_txn_manager,
                                     common::ManagedPointer(&thread_registry_), &block_store_);
    recovery_manager.StartRecovery();
    recovery_manager.WaitForRecoveryToFinish();

    // We need to start the log manager because we will be doing queries into the LargeSqlTableTestObject's catalog
    log_manager.Start();

    // Check we recovered all the original tables
    for (auto &database_oid : tested->GetDatabases()) {
      for (auto &table_oid : tested->GetTablesForDatabase(database_oid)) {
        // Get original sql table
        auto original_txn = tested->GetTxnManager()->BeginTransaction();
        auto original_sql_table = tested->GetTable(original_txn, database_oid, table_oid);

        // Get Recovered table
        auto *recovery_txn = recovery_txn_manager.BeginTransaction();
        auto db_catalog = recovered_catalog.GetDatabaseCatalog(recovery_txn, database_oid);
        EXPECT_TRUE(db_catalog != nullptr);
        auto recovered_sql_table = db_catalog->GetTable(recovery_txn, table_oid);
        EXPECT_TRUE(recovered_sql_table != nullptr);

        EXPECT_TRUE(StorageTestUtil::SqlTableEqualDeep(
            original_sql_table->table_.layout, original_sql_table, recovered_sql_table,
            tested->GetTupleSlotsForTable(database_oid, table_oid), recovery_manager.tuple_slot_map_,
            tested->GetTxnManager(), &recovery_txn_manager));
        tested->GetTxnManager()->Commit(original_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
        recovery_txn_manager.Commit(recovery_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
      }
    }

    // Clean up test object and recovered catalogs
    delete tested;
    log_manager.PersistAndStop();
    GarbageCollectCatalogObject(&recovered_catalog, &recovery_txn_manager);
  }
};

// This test inserts some tuples into a single table. It then recreates the test table from
// the log, and verifies that this new table is the same as the original table
// NOLINTNEXTLINE
TEST_F(RecoveryTests, SingleTableTest) {
  LargeSqlTableTestConfiguration config = LargeSqlTableTestConfiguration::Builder()
                                              .SetNumDatabases(1)
                                              .SetNumTables(1)
                                              .SetMaxColumns(5)
                                              .SetInitialTableSize(1000)
                                              .SetTxnLength(5)
                                              .SetInsertUpdateSelectDeleteRatio({0.2, 0.5, 0.2, 0.1})
                                              .SetVarlenAllowed(true)
                                              .build();
  RecoveryTests::RunTest(config);
}

// This test checks that we recover correctly in a high abort rate workload. We achieve the high abort rate by having
// large transaction lengths (number of updates). Further, to ensure that more aborted transactions flush logs before
// aborting, we have transactions make large updates (by having high number columns). This will cause RedoBuffers to
// fill quickly.
// NOLINTNEXTLINE
TEST_F(RecoveryTests, HighAbortRateTest) {
  LargeSqlTableTestConfiguration config = LargeSqlTableTestConfiguration::Builder()
                                              .SetNumDatabases(1)
                                              .SetNumTables(1)
                                              .SetMaxColumns(1000)
                                              .SetInitialTableSize(1000)
                                              .SetTxnLength(20)
                                              .SetInsertUpdateSelectDeleteRatio({0.2, 0.5, 0.3, 0.0})
                                              .SetVarlenAllowed(true)
                                              .build();
  RecoveryTests::RunTest(config);
}

// This test inserts some tuples into multiple tables across multiple databases. It then recovers these tables, and
// verifies that the recovered tables are equal to the test tables.
// NOLINTNEXTLINE
TEST_F(RecoveryTests, MultiDatabaseTest) {
  LargeSqlTableTestConfiguration config = LargeSqlTableTestConfiguration::Builder()
                                              .SetNumDatabases(3)
                                              .SetNumTables(5)
                                              .SetMaxColumns(5)
                                              .SetInitialTableSize(100)
                                              .SetTxnLength(5)
                                              .SetInsertUpdateSelectDeleteRatio({0.3, 0.6, 0.0, 0.1})
                                              .SetVarlenAllowed(true)
                                              .build();
  RecoveryTests::RunTest(config);
}

// Tests that we correctly process records corresponding to a drop database command.
// NOLINTNEXTLINE
TEST_F(RecoveryTests, DropDatabaseTest) {
  std::string database_name = "testdb";
  // Bring up original components
  LogManager log_manager(LOG_FILE_NAME, num_log_buffers_, log_serialization_interval_, log_persist_interval_,
                         log_persist_threshold_, &pool_, common::ManagedPointer(&thread_registry_));
  log_manager.Start();
  transaction::TransactionManager txn_manager{&pool_, true, &log_manager};
  catalog::Catalog catalog(&txn_manager, &block_store_);

  // Create database
  auto *txn = txn_manager.BeginTransaction();
  auto db_oid = catalog.CreateDatabase(txn, database_name, true /* bootstrap */);
  EXPECT_TRUE(db_oid != catalog::INVALID_DATABASE_OID);
  txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Drop the database
  txn = txn_manager.BeginTransaction();
  EXPECT_TRUE(catalog.DeleteDatabase(txn, db_oid));
  txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  EXPECT_EQ(catalog::INVALID_DATABASE_OID, catalog.GetDatabaseOid(txn, database_name));
  EXPECT_FALSE(catalog.GetDatabaseCatalog(txn, db_oid));

  // Simulate the system "shutting down". Guarantee persist of log records
  log_manager.PersistAndStop();

  /* We now "boot up" up the system and start recovery */

  // Start a transaction manager with logging disabled, we don't want to log the log replaying
  transaction::TransactionManager recovery_txn_manager{&pool_, true, LOGGING_DISABLED};

  // Create catalog for recovery
  catalog::Catalog recovered_catalog(&recovery_txn_manager, &block_store_);

  // Instantiate recovery manager, and recover the catalog.
  DiskLogProvider log_provider(LOG_FILE_NAME);
  RecoveryManager recovery_manager(&log_provider, common::ManagedPointer(&recovered_catalog), &recovery_txn_manager,
                                   common::ManagedPointer(&thread_registry_), &block_store_);
  recovery_manager.StartRecovery();
  recovery_manager.WaitForRecoveryToFinish();

  // Assert the database we deleted doesn't exist
  txn = recovery_txn_manager.BeginTransaction();
  EXPECT_EQ(catalog::INVALID_DATABASE_OID, recovered_catalog.GetDatabaseOid(txn, database_name));
  EXPECT_FALSE(recovered_catalog.GetDatabaseCatalog(txn, db_oid));
  recovery_txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  log_manager.Start();
  GarbageCollectCatalogObject(&catalog, &txn_manager);
  log_manager.PersistAndStop();
  GarbageCollectCatalogObject(&recovered_catalog, &recovery_txn_manager);
}

// Tests that we correctly process records corresponding to a drop table command.
// NOLINTNEXTLINE
TEST_F(RecoveryTests, DropTableTest) {
  std::string database_name = "testdb";
  auto namespace_oid = catalog::NAMESPACE_DEFAULT_NAMESPACE_OID;
  std::string table_name = "testtable";
  // Bring up original components
  LogManager log_manager(LOG_FILE_NAME, num_log_buffers_, log_serialization_interval_, log_persist_interval_,
                         log_persist_threshold_, &pool_, common::ManagedPointer(&thread_registry_));
  log_manager.Start();
  transaction::TransactionManager txn_manager{&pool_, true, &log_manager};
  catalog::Catalog catalog(&txn_manager, &block_store_);

  // Create database
  auto *txn = txn_manager.BeginTransaction();
  auto db_oid = catalog.CreateDatabase(txn, database_name, true /* bootstrap */);
  EXPECT_TRUE(db_oid != catalog::INVALID_DATABASE_OID);
  txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Create the table
  txn = txn_manager.BeginTransaction();
  auto db_catalog = catalog.GetDatabaseCatalog(txn, db_oid);
  auto *table_schema = StorageTestUtil::RandomSchemaNoVarlen(5, &generator_);
  auto table_oid = db_catalog->CreateTable(txn, namespace_oid, table_name, *table_schema);
  EXPECT_TRUE(table_oid != catalog::INVALID_TABLE_OID);
  auto *table_ptr = new storage::SqlTable(&block_store_, *table_schema);
  EXPECT_TRUE(db_catalog->SetTablePointer(txn, table_oid, table_ptr));
  txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Drop the table
  txn = txn_manager.BeginTransaction();
  EXPECT_TRUE(db_catalog->DeleteTable(txn, table_oid));
  txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Simulate the system "shutting down". Guarantee persist of log records
  log_manager.PersistAndStop();

  /* We now "boot up" up the system and start recovery */

  // Start a transaction manager with logging disabled, we don't want to log the log replaying
  transaction::TransactionManager recovery_txn_manager{&pool_, true, LOGGING_DISABLED};

  // Create catalog for recovery
  catalog::Catalog recovered_catalog(&recovery_txn_manager, &block_store_);

  // Instantiate recovery manager, and recover the catalog.
  DiskLogProvider log_provider(LOG_FILE_NAME);
  RecoveryManager recovery_manager(&log_provider, common::ManagedPointer(&recovered_catalog), &recovery_txn_manager,
                                   common::ManagedPointer(&thread_registry_), &block_store_);
  recovery_manager.StartRecovery();
  recovery_manager.WaitForRecoveryToFinish();

  // Assert the database we created exists
  txn = recovery_txn_manager.BeginTransaction();
  EXPECT_EQ(db_oid, recovered_catalog.GetDatabaseOid(txn, database_name));
  db_catalog = recovered_catalog.GetDatabaseCatalog(txn, db_oid);
  EXPECT_TRUE(db_catalog);

  // Assert the table we deleted doesn't exist
  EXPECT_EQ(catalog::INVALID_TABLE_OID, db_catalog->GetTableOid(txn, namespace_oid, table_name));
  EXPECT_FALSE(db_catalog->GetTable(txn, table_oid));
  recovery_txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  log_manager.Start();
  GarbageCollectCatalogObject(&catalog, &txn_manager);
  log_manager.PersistAndStop();
  GarbageCollectCatalogObject(&recovered_catalog, &recovery_txn_manager);
  delete table_schema;
}

// Tests that we correctly process records corresponding to a drop index command.
// NOLINTNEXTLINE
TEST_F(RecoveryTests, DropIndexTest) {
  std::string database_name = "testdb";
  auto namespace_oid = catalog::NAMESPACE_DEFAULT_NAMESPACE_OID;
  std::string table_name = "testtable";
  std::string index_name = "testindex";
  // Bring up original components
  LogManager log_manager(LOG_FILE_NAME, num_log_buffers_, log_serialization_interval_, log_persist_interval_,
                         log_persist_threshold_, &pool_, common::ManagedPointer(&thread_registry_));
  log_manager.Start();
  transaction::TransactionManager txn_manager{&pool_, true, &log_manager};
  catalog::Catalog catalog(&txn_manager, &block_store_);

  // Create database
  auto *txn = txn_manager.BeginTransaction();
  auto db_oid = catalog.CreateDatabase(txn, database_name, true /* bootstrap */);
  EXPECT_TRUE(db_oid != catalog::INVALID_DATABASE_OID);
  txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Create the table
  txn = txn_manager.BeginTransaction();
  auto db_catalog = catalog.GetDatabaseCatalog(txn, db_oid);
  auto table_schema = StorageTestUtil::RandomSchemaNoVarlen(5, &generator_);
  auto table_oid = db_catalog->CreateTable(txn, namespace_oid, table_name, *table_schema);
  EXPECT_TRUE(table_oid != catalog::INVALID_TABLE_OID);
  auto *table_ptr = new storage::SqlTable(&block_store_, *table_schema);
  EXPECT_TRUE(db_catalog->SetTablePointer(txn, table_oid, table_ptr));
  txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Create the index
  txn = txn_manager.BeginTransaction();
  auto index_schema = DummyIndexSchema();
  auto index_oid = db_catalog->CreateIndex(txn, namespace_oid, index_name, table_oid, index_schema);
  EXPECT_TRUE(index_oid != catalog::INVALID_INDEX_OID);
  auto *index_ptr = storage::index::IndexBuilder()
                        .SetConstraintType(storage::index::ConstraintType::UNIQUE)
                        .SetKeySchema(index_schema)
                        .SetOid(index_oid)
                        .Build();
  EXPECT_TRUE(db_catalog->SetIndexPointer(txn, index_oid, index_ptr));
  txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Drop the index
  txn = txn_manager.BeginTransaction();
  EXPECT_TRUE(db_catalog->DeleteIndex(txn, index_oid));
  txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Simulate the system "shutting down". Guarantee persist of log records
  log_manager.PersistAndStop();

  /* We now "boot up" up the system and start recovery */

  // Start a transaction manager with logging disabled, we don't want to log the log replaying
  transaction::TransactionManager recovery_txn_manager{&pool_, true, LOGGING_DISABLED};

  // Create catalog for recovery
  catalog::Catalog recovered_catalog(&recovery_txn_manager, &block_store_);

  // Instantiate recovery manager, and recover the catalog.
  DiskLogProvider log_provider(LOG_FILE_NAME);
  RecoveryManager recovery_manager(&log_provider, common::ManagedPointer(&recovered_catalog), &recovery_txn_manager,
                                   common::ManagedPointer(&thread_registry_), &block_store_);
  recovery_manager.StartRecovery();
  recovery_manager.WaitForRecoveryToFinish();

  // Assert the database we created exists
  txn = recovery_txn_manager.BeginTransaction();
  EXPECT_EQ(db_oid, recovered_catalog.GetDatabaseOid(txn, database_name));
  db_catalog = recovered_catalog.GetDatabaseCatalog(txn, db_oid);
  EXPECT_TRUE(db_catalog);

  // Assert the table we created exists
  EXPECT_EQ(table_oid, db_catalog->GetTableOid(txn, namespace_oid, table_name));
  EXPECT_TRUE(db_catalog->GetTable(txn, table_oid));

  // Assert the index we deleted doesn't exist
  EXPECT_EQ(0, db_catalog->GetIndexes(txn, table_oid).size());
  EXPECT_FALSE(db_catalog->GetIndex(txn, index_oid));
  recovery_txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  log_manager.Start();
  GarbageCollectCatalogObject(&catalog, &txn_manager);
  log_manager.PersistAndStop();
  GarbageCollectCatalogObject(&recovered_catalog, &recovery_txn_manager);
  delete table_schema;
}

// Tests that we correctly process records corresponding to a drop namespace command.
// NOLINTNEXTLINE
TEST_F(RecoveryTests, DropNamespaceTest) {
  std::string database_name = "testdb";
  std::string namespace_name = "testnamespace";
  // Bring up original components
  LogManager log_manager(LOG_FILE_NAME, num_log_buffers_, log_serialization_interval_, log_persist_interval_,
                         log_persist_threshold_, &pool_, common::ManagedPointer(&thread_registry_));
  log_manager.Start();
  transaction::TransactionManager txn_manager{&pool_, true, &log_manager};
  catalog::Catalog catalog(&txn_manager, &block_store_);

  // Create database
  auto *txn = txn_manager.BeginTransaction();
  auto db_oid = catalog.CreateDatabase(txn, database_name, true /* bootstrap */);
  EXPECT_TRUE(db_oid != catalog::INVALID_DATABASE_OID);
  txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Create the namespace
  txn = txn_manager.BeginTransaction();
  auto db_catalog = catalog.GetDatabaseCatalog(txn, db_oid);
  auto namespace_oid = db_catalog->CreateNamespace(txn, namespace_name);
  EXPECT_TRUE(namespace_oid != catalog::INVALID_NAMESPACE_OID);
  txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Drop the namespace
  txn = txn_manager.BeginTransaction();
  EXPECT_TRUE(db_catalog->DeleteNamespace(txn, namespace_oid));
  txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Simulate the system "shutting down". Guarantee persist of log records
  log_manager.PersistAndStop();

  /* We now "boot up" up the system and start recovery */

  // Start a transaction manager with logging disabled, we don't want to log the log replaying
  transaction::TransactionManager recovery_txn_manager{&pool_, true, LOGGING_DISABLED};

  // Create catalog for recovery
  catalog::Catalog recovered_catalog(&recovery_txn_manager, &block_store_);

  // Instantiate recovery manager, and recover the catalog.
  DiskLogProvider log_provider(LOG_FILE_NAME);
  RecoveryManager recovery_manager(&log_provider, common::ManagedPointer(&recovered_catalog), &recovery_txn_manager,
                                   common::ManagedPointer(&thread_registry_), &block_store_);
  recovery_manager.StartRecovery();
  recovery_manager.WaitForRecoveryToFinish();

  // Assert the database we created exists
  txn = recovery_txn_manager.BeginTransaction();
  EXPECT_EQ(db_oid, recovered_catalog.GetDatabaseOid(txn, database_name));
  db_catalog = recovered_catalog.GetDatabaseCatalog(txn, db_oid);
  EXPECT_TRUE(db_catalog);

  // Assert the namespace we deleted doesn't exist
  EXPECT_EQ(catalog::INVALID_NAMESPACE_OID, db_catalog->GetNamespaceOid(txn, namespace_name));
  recovery_txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  log_manager.Start();
  GarbageCollectCatalogObject(&catalog, &txn_manager);
  log_manager.PersistAndStop();
  GarbageCollectCatalogObject(&recovered_catalog, &recovery_txn_manager);
}

// Tests that we correctly process cascading deletes originating from a drop database command. This means cascading
// deletes of tables and indexes
// NOLINTNEXTLINE
TEST_F(RecoveryTests, DropDatabaseCascadeDeleteTest) {
  std::string database_name = "testdb";
  auto namespace_oid = catalog::NAMESPACE_DEFAULT_NAMESPACE_OID;
  std::string table_name = "testtable";
  std::string index_name = "testindex";
  // Bring up original components
  LogManager log_manager(LOG_FILE_NAME, num_log_buffers_, log_serialization_interval_, log_persist_interval_,
                         log_persist_threshold_, &pool_, common::ManagedPointer(&thread_registry_));
  log_manager.Start();
  transaction::TransactionManager txn_manager{&pool_, true, &log_manager};
  catalog::Catalog catalog(&txn_manager, &block_store_);

  // Create database
  auto *txn = txn_manager.BeginTransaction();
  auto db_oid = catalog.CreateDatabase(txn, database_name, true /* bootstrap */);
  EXPECT_TRUE(db_oid != catalog::INVALID_DATABASE_OID);
  txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Create the table
  txn = txn_manager.BeginTransaction();
  auto db_catalog = catalog.GetDatabaseCatalog(txn, db_oid);
  auto table_schema = StorageTestUtil::RandomSchemaNoVarlen(5, &generator_);
  auto table_oid = db_catalog->CreateTable(txn, namespace_oid, table_name, *table_schema);
  EXPECT_TRUE(table_oid != catalog::INVALID_TABLE_OID);
  auto *table_ptr = new storage::SqlTable(&block_store_, *table_schema);
  EXPECT_TRUE(db_catalog->SetTablePointer(txn, table_oid, table_ptr));
  txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Create an index
  txn = txn_manager.BeginTransaction();
  auto index_schema = DummyIndexSchema();
  auto index_oid = db_catalog->CreateIndex(txn, namespace_oid, index_name, table_oid, index_schema);
  EXPECT_TRUE(index_oid != catalog::INVALID_INDEX_OID);
  auto *index_ptr = storage::index::IndexBuilder()
                        .SetConstraintType(storage::index::ConstraintType::UNIQUE)
                        .SetKeySchema(index_schema)
                        .SetOid(index_oid)
                        .Build();
  EXPECT_TRUE(db_catalog->SetIndexPointer(txn, index_oid, index_ptr));
  txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Drop the database
  txn = txn_manager.BeginTransaction();
  EXPECT_TRUE(catalog.DeleteDatabase(txn, db_oid));
  txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  EXPECT_EQ(catalog::INVALID_DATABASE_OID, catalog.GetDatabaseOid(txn, database_name));
  EXPECT_FALSE(catalog.GetDatabaseCatalog(txn, db_oid));

  // Simulate the system "shutting down". Guarantee persist of log records
  log_manager.PersistAndStop();

  /* We now "boot up" up the system and start recovery */

  // Start a transaction manager with logging disabled, we don't want to log the log replaying
  transaction::TransactionManager recovery_txn_manager{&pool_, true, LOGGING_DISABLED};

  // Create catalog for recovery
  catalog::Catalog recovered_catalog(&recovery_txn_manager, &block_store_);

  // Instantiate recovery manager, and recover the catalog.
  DiskLogProvider log_provider(LOG_FILE_NAME);
  RecoveryManager recovery_manager(&log_provider, common::ManagedPointer(&recovered_catalog), &recovery_txn_manager,
                                   common::ManagedPointer(&thread_registry_), &block_store_);
  recovery_manager.StartRecovery();
  recovery_manager.WaitForRecoveryToFinish();

  // Assert the database does not exist
  txn = recovery_txn_manager.BeginTransaction();
  EXPECT_EQ(catalog::INVALID_DATABASE_OID, recovered_catalog.GetDatabaseOid(txn, database_name));
  EXPECT_FALSE(recovered_catalog.GetDatabaseCatalog(txn, db_oid));
  recovery_txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  log_manager.Start();
  GarbageCollectCatalogObject(&catalog, &txn_manager);
  log_manager.PersistAndStop();
  GarbageCollectCatalogObject(&recovered_catalog, &recovery_txn_manager);
  delete table_schema;
}
}  // namespace terrier::storage
