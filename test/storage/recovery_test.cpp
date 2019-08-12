#include <unordered_map>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/postgres/pg_namespace.h"
#include "gtest/gtest.h"
#include "main/db_main.h"
#include "storage/garbage_collector_thread.h"
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

  void RunTest(const LargeSqlTableTestConfiguration &config) {
    // Initialize table and run workload with logging enabled
    LogManager log_manager(LOG_FILE_NAME, num_log_buffers_, log_serialization_interval_, log_persist_interval_,
                           log_persist_threshold_, &pool_, common::ManagedPointer(&thread_registry_));
    log_manager.Start();
    auto *tested = new LargeSqlTableTestObject(config, &block_store_, &pool_, &generator_, &log_manager);

    // Run transactions and fully persist all changes
    tested->SimulateOltp(100, 4);
    log_manager.PersistAndStop();

    /* CRASH */

    // Start a transaction manager with logging disabled, we don't want to log the log replaying
    transaction::TransactionManager recovery_txn_manager{&pool_, true, LOGGING_DISABLED};

    // Create catalog for recovery
    catalog::Catalog recovered_catalog(&recovery_txn_manager, &block_store_);

    // Instantiate recovery manager, and recover the tables.
    DiskLogProvider log_provider(LOG_FILE_NAME);
    RecoveryManager recovery_manager(&log_provider, common::ManagedPointer(&recovered_catalog), &recovery_txn_manager,
                                     common::ManagedPointer(&thread_registry_), &block_store_);
    recovery_manager.StartRecovery();
    recovery_manager.FinishRecovery();

    // We need to start the log manager because we will be doing queries into the LargeSqlTableTestObject's catalog
    log_manager.Start();

    // Check we recovered all the original tables
    for (auto &database_oid : tested->GetDatabases()) {
      for (auto &table_oid : tested->GetTablesForDatabase(database_oid)) {
        // Get original sql table
        auto original_sql_table = tested->GetTable(database_oid, table_oid);

        // Get Recovered table
        auto *txn = recovery_txn_manager.BeginTransaction();
        auto db_catalog = recovered_catalog.GetDatabaseCatalog(txn, database_oid);
        EXPECT_TRUE(db_catalog != nullptr);
        auto recovered_sql_table = db_catalog->GetTable(txn, table_oid);
        EXPECT_TRUE(recovered_sql_table != nullptr);
        recovery_txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

        EXPECT_TRUE(StorageTestUtil::SqlTableEqualDeep(original_sql_table->Layout(), original_sql_table,
                                                       recovered_sql_table,
                                                       tested->GetTupleSlotsForTable(database_oid, table_oid),
                                                       recovery_manager.tuple_slot_map_, &recovery_txn_manager));
      }
    }

    // Clean up test object
    delete tested;
    log_manager.PersistAndStop();

    // Perform GC 3 times to fully clean up catalog
    recovered_catalog.TearDown();
    gc_ = new storage::GarbageCollector(&recovery_txn_manager, nullptr);
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
    delete gc_;
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
                                              .SetUpdateSelectDeleteRatio({0.7, 0.2, 0.1})
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
                                              .SetUpdateSelectDeleteRatio({0.7, 0.3, 0.0})
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
                                              .SetUpdateSelectDeleteRatio({0.9, 0.0, 0.1})
                                              .SetVarlenAllowed(true)
                                              .build();
  RecoveryTests::RunTest(config);
}

// TODO(Gus): Add an abort test

// Tests that we correctly process records corresponding to a drop database command.
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

  // Guarantee persist of log records
  log_manager.PersistAndStop();

  /* CRASH */

  // Start a transaction manager with logging disabled, we don't want to log the log replaying
  transaction::TransactionManager recovery_txn_manager{&pool_, true, LOGGING_DISABLED};

  // Create catalog for recovery
  catalog::Catalog recovered_catalog(&recovery_txn_manager, &block_store_);

  // Instantiate recovery manager, and recover the catalog.
  DiskLogProvider log_provider(LOG_FILE_NAME);
  RecoveryManager recovery_manager(&log_provider, common::ManagedPointer(&recovered_catalog), &recovery_txn_manager,
                                   common::ManagedPointer(&thread_registry_), &block_store_);
  recovery_manager.StartRecovery();
  recovery_manager.FinishRecovery();

  // Assert the database we deleted doesn't exist
  txn = recovery_txn_manager.BeginTransaction();
  EXPECT_EQ(catalog::INVALID_DATABASE_OID, recovered_catalog.GetDatabaseOid(txn, database_name));
  EXPECT_FALSE(recovered_catalog.GetDatabaseCatalog(txn, db_oid));
}

// Tests that we correctly process records corresponding to a drop table command.
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
  auto table_oid =
      db_catalog->CreateTable(txn, namespace_oid, table_name, *(StorageTestUtil::RandomSchemaNoVarlen(5, &generator_)));
  EXPECT_TRUE(table_oid != catalog::INVALID_TABLE_OID);
  txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Drop the table
  txn = txn_manager.BeginTransaction();
  EXPECT_TRUE(db_catalog->DeleteTable(txn, table_oid));
  txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Guarantee persist of log records
  log_manager.PersistAndStop();

  /* CRASH */

  // Start a transaction manager with logging disabled, we don't want to log the log replaying
  transaction::TransactionManager recovery_txn_manager{&pool_, true, LOGGING_DISABLED};

  // Create catalog for recovery
  catalog::Catalog recovered_catalog(&recovery_txn_manager, &block_store_);

  // Instantiate recovery manager, and recover the catalog.
  DiskLogProvider log_provider(LOG_FILE_NAME);
  RecoveryManager recovery_manager(&log_provider, common::ManagedPointer(&recovered_catalog), &recovery_txn_manager,
                                   common::ManagedPointer(&thread_registry_), &block_store_);
  recovery_manager.StartRecovery();
  recovery_manager.FinishRecovery();

  // Assert the database we created exists
  txn = recovery_txn_manager.BeginTransaction();
  EXPECT_EQ(db_oid, recovered_catalog.GetDatabaseOid(txn, database_name));
  db_catalog = recovered_catalog.GetDatabaseCatalog(txn, db_oid);
  EXPECT_TRUE(db_catalog);

  // Assert the table we deleted doesn't exist
  EXPECT_EQ(catalog::INVALID_TABLE_OID, db_catalog->GetTableOid(txn, namespace_oid, table_name));
  EXPECT_FALSE(db_catalog->GetTable(txn, table_oid));
  recovery_txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// Tests that we correctly process records corresponding to a drop namespace command.
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

  // Guarantee persist of log records
  log_manager.PersistAndStop();

  /* CRASH */

  // Start a transaction manager with logging disabled, we don't want to log the log replaying
  transaction::TransactionManager recovery_txn_manager{&pool_, true, LOGGING_DISABLED};

  // Create catalog for recovery
  catalog::Catalog recovered_catalog(&recovery_txn_manager, &block_store_);

  // Instantiate recovery manager, and recover the catalog.
  DiskLogProvider log_provider(LOG_FILE_NAME);
  RecoveryManager recovery_manager(&log_provider, common::ManagedPointer(&recovered_catalog), &recovery_txn_manager,
                                   common::ManagedPointer(&thread_registry_), &block_store_);
  recovery_manager.StartRecovery();
  recovery_manager.FinishRecovery();

  // Assert the database we created exists
  txn = recovery_txn_manager.BeginTransaction();
  EXPECT_EQ(db_oid, recovered_catalog.GetDatabaseOid(txn, database_name));
  db_catalog = recovered_catalog.GetDatabaseCatalog(txn, db_oid);
  EXPECT_TRUE(db_catalog);

  // Assert the namespace we deleted doesn't exist
  EXPECT_EQ(catalog::INVALID_NAMESPACE_OID, db_catalog->GetNamespaceOid(txn, namespace_name));
  recovery_txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// Tests that we correctly process cascading deletes originating from a drop database command.
TEST_F(RecoveryTests, DropDatabaseCascadeDeleteTest) {
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
  auto table_oid =
      db_catalog->CreateTable(txn, namespace_oid, table_name, *(StorageTestUtil::RandomSchemaNoVarlen(5, &generator_)));
  EXPECT_TRUE(table_oid != catalog::INVALID_TABLE_OID);
  txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Drop the database
  txn = txn_manager.BeginTransaction();
  EXPECT_TRUE(catalog.DeleteDatabase(txn, db_oid));
  txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  EXPECT_EQ(catalog::INVALID_DATABASE_OID, catalog.GetDatabaseOid(txn, database_name));
  EXPECT_FALSE(catalog.GetDatabaseCatalog(txn, db_oid));

  // Guarantee persist of log records
  log_manager.PersistAndStop();

  /* CRASH */

  // Start a transaction manager with logging disabled, we don't want to log the log replaying
  transaction::TransactionManager recovery_txn_manager{&pool_, true, LOGGING_DISABLED};

  // Create catalog for recovery
  catalog::Catalog recovered_catalog(&recovery_txn_manager, &block_store_);

  // Instantiate recovery manager, and recover the catalog.
  DiskLogProvider log_provider(LOG_FILE_NAME);
  RecoveryManager recovery_manager(&log_provider, common::ManagedPointer(&recovered_catalog), &recovery_txn_manager,
                                   common::ManagedPointer(&thread_registry_), &block_store_);
  recovery_manager.StartRecovery();
  recovery_manager.FinishRecovery();

  // Assert the database does not exist
  txn = recovery_txn_manager.BeginTransaction();
  EXPECT_EQ(catalog::INVALID_DATABASE_OID, recovered_catalog.GetDatabaseOid(txn, database_name));
  EXPECT_FALSE(recovered_catalog.GetDatabaseCatalog(txn, db_oid));
}
}  // namespace terrier::storage
