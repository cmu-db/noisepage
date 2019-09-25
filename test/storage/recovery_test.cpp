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
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "util/catalog_test_util.h"
#include "util/sql_table_test_util.h"
#include "util/storage_test_util.h"
#include "util/test_harness.h"

// Make sure that if you create additional files, you call unlink on them after the test finishes. Otherwise, repeated
// executions will read old test's data, and the cause of the errors will be hard to identify. Trust me it will drive
// you nuts...
#define LOG_FILE_NAME "./test.log"

namespace terrier::storage {
class RecoveryTests : public TerrierTest {
 protected:
  // Settings for log manager
  const uint64_t num_log_buffers_ = 100;
  const std::chrono::microseconds log_serialization_interval_{10};
  const std::chrono::milliseconds log_persist_interval_{20};
  const uint64_t log_persist_threshold_ = (1 << 20);  // 1MB

  std::default_random_engine generator_;
  storage::RecordBufferSegmentPool buffer_pool_{2000, 100};
  storage::BlockStore block_store_{100, 100};

  // Settings for gc
  const std::chrono::milliseconds gc_period_{10};

  // Original Components
  common::DedicatedThreadRegistry thread_registry_;
  LogManager *log_manager_;
  transaction::TimestampManager *timestamp_manager_;
  transaction::DeferredActionManager *deferred_action_manager_;
  transaction::TransactionManager *txn_manager_;
  catalog::Catalog *catalog_;
  storage::GarbageCollector *gc_;
  storage::GarbageCollectorThread *gc_thread_;

  // Recovery Components
  transaction::TimestampManager *recovery_timestamp_manager_;
  transaction::DeferredActionManager *recovery_deferred_action_manager_;
  transaction::TransactionManager *recovery_txn_manager_;
  catalog::Catalog *recovery_catalog_;
  storage::GarbageCollector *recovery_gc_;
  storage::GarbageCollectorThread *recovery_gc_thread_;

  void SetUp() override {
    TerrierTest::SetUp();
    // Unlink log file incase one exists from previous test iteration
    unlink(LOG_FILE_NAME);
    log_manager_ = new LogManager(LOG_FILE_NAME, num_log_buffers_, log_serialization_interval_, log_persist_interval_,
                                  log_persist_threshold_, &buffer_pool_, common::ManagedPointer(&thread_registry_));
    log_manager_->Start();
    timestamp_manager_ = new transaction::TimestampManager;
    deferred_action_manager_ = new transaction::DeferredActionManager(timestamp_manager_);
    txn_manager_ = new transaction::TransactionManager(timestamp_manager_, deferred_action_manager_, &buffer_pool_,
                                                       true, log_manager_);
    catalog_ = new catalog::Catalog(txn_manager_, &block_store_);
    gc_ = new storage::GarbageCollector(timestamp_manager_, deferred_action_manager_, txn_manager_, DISABLED);
    gc_thread_ = new storage::GarbageCollectorThread(gc_, gc_period_);  // Enable background GC

    recovery_timestamp_manager_ = new transaction::TimestampManager;
    recovery_deferred_action_manager_ = new transaction::DeferredActionManager(recovery_timestamp_manager_);
    recovery_txn_manager_ = new transaction::TransactionManager(
        recovery_timestamp_manager_, recovery_deferred_action_manager_, &buffer_pool_, true, DISABLED);
    recovery_catalog_ = new catalog::Catalog(recovery_txn_manager_, &block_store_);
    recovery_gc_ = new storage::GarbageCollector(recovery_timestamp_manager_, recovery_deferred_action_manager_,
                                                 recovery_txn_manager_, DISABLED);
    recovery_gc_thread_ = new storage::GarbageCollectorThread(recovery_gc_, gc_period_);  // Enable background GC
  }

  void TearDown() override {
    // Delete log file
    unlink(LOG_FILE_NAME);
    TerrierTest::TearDown();

    // Destroy recovered catalog if the test has not cleaned it up already
    if (recovery_catalog_ != nullptr) {
      recovery_catalog_->TearDown();
      delete recovery_gc_thread_;
      StorageTestUtil::FullyPerformGC(recovery_gc_, DISABLED);
    }

    // Destroy original catalog. We need to manually call GC followed by a ForceFlush because catalog deletion can defer
    // events that create new transactions, which then need to be flushed before they can be GC'd.
    catalog_->TearDown();
    delete gc_thread_;
    StorageTestUtil::FullyPerformGC(gc_, log_manager_);
    log_manager_->PersistAndStop();

    delete recovery_gc_;
    delete recovery_catalog_;
    delete recovery_txn_manager_;
    delete recovery_deferred_action_manager_;
    delete recovery_timestamp_manager_;
    delete gc_;
    delete catalog_;
    delete txn_manager_;
    delete deferred_action_manager_;
    delete timestamp_manager_;
    delete log_manager_;
  }

  catalog::IndexSchema DummyIndexSchema() {
    std::vector<catalog::IndexSchema::Column> keycols;
    keycols.emplace_back(
        "", type::TypeId::INTEGER, false,
        parser::ColumnValueExpression(catalog::db_oid_t(0), catalog::table_oid_t(0), catalog::col_oid_t(1)));
    StorageTestUtil::ForceOid(&(keycols[0]), catalog::indexkeycol_oid_t(1));
    return catalog::IndexSchema(keycols, storage::index::IndexType::BWTREE, true, true, false, true);
  }

  catalog::db_oid_t CreateDatabase(transaction::TransactionContext *txn, catalog::Catalog *catalog,
                                   const std::string &database_name) {
    auto db_oid = catalog->CreateDatabase(txn, database_name, true /* bootstrap */);
    EXPECT_TRUE(db_oid != catalog::INVALID_DATABASE_OID);
    return db_oid;
  }

  void DropDatabase(transaction::TransactionContext *txn, catalog::Catalog *catalog, const catalog::db_oid_t db_oid) {
    EXPECT_TRUE(catalog->DeleteDatabase(txn, db_oid));
    EXPECT_FALSE(catalog->GetDatabaseCatalog(txn, db_oid));
  }

  catalog::table_oid_t CreateTable(transaction::TransactionContext *txn,
                                   common::ManagedPointer<catalog::DatabaseCatalog> db_catalog,
                                   const catalog::namespace_oid_t ns_oid, const std::string &table_name) {
    auto col = catalog::Schema::Column(
        "attribute", type::TypeId::INTEGER, false,
        parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
    auto table_schema = catalog::Schema(std::vector<catalog::Schema::Column>({col}));
    auto table_oid = db_catalog->CreateTable(txn, ns_oid, table_name, table_schema);
    EXPECT_TRUE(table_oid != catalog::INVALID_TABLE_OID);
    const auto catalog_schema = db_catalog->GetSchema(txn, table_oid);
    auto *table_ptr = new storage::SqlTable(&block_store_, catalog_schema);
    EXPECT_TRUE(db_catalog->SetTablePointer(txn, table_oid, table_ptr));
    return table_oid;
  }

  void DropTable(transaction::TransactionContext *txn, common::ManagedPointer<catalog::DatabaseCatalog> db_catalog,
                 const catalog::table_oid_t table_oid) {
    EXPECT_TRUE(db_catalog->DeleteTable(txn, table_oid));
  }

  catalog::index_oid_t CreateIndex(transaction::TransactionContext *txn,
                                   common::ManagedPointer<catalog::DatabaseCatalog> db_catalog,
                                   const catalog::namespace_oid_t ns_oid, const catalog::table_oid_t table_oid,
                                   const std::string &index_name) {
    auto index_schema = DummyIndexSchema();
    auto index_oid = db_catalog->CreateIndex(txn, ns_oid, index_name, table_oid, index_schema);
    EXPECT_TRUE(index_oid != catalog::INVALID_INDEX_OID);
    auto *index_ptr = storage::index::IndexBuilder().SetKeySchema(index_schema).Build();
    EXPECT_TRUE(db_catalog->SetIndexPointer(txn, index_oid, index_ptr));
    return index_oid;
  }

  void DropIndex(transaction::TransactionContext *txn, common::ManagedPointer<catalog::DatabaseCatalog> db_catalog,
                 const catalog::index_oid_t index_oid) {
    EXPECT_TRUE(db_catalog->DeleteIndex(txn, index_oid));
  }

  catalog::namespace_oid_t CreateNamespace(transaction::TransactionContext *txn,
                                           common::ManagedPointer<catalog::DatabaseCatalog> db_catalog,
                                           const std::string &namespace_name) {
    auto namespace_oid = db_catalog->CreateNamespace(txn, namespace_name);
    EXPECT_TRUE(namespace_oid != catalog::INVALID_NAMESPACE_OID);
    return namespace_oid;
  }

  void DropNamespace(transaction::TransactionContext *txn, common::ManagedPointer<catalog::DatabaseCatalog> db_catalog,
                     const catalog::namespace_oid_t ns_oid) {
    EXPECT_TRUE(db_catalog->DeleteNamespace(txn, ns_oid));
  }

  storage::RedoBuffer &GetRedoBuffer(transaction::TransactionContext *txn) { return txn->redo_buffer_; }

  storage::BlockLayout &GetBlockLayout(common::ManagedPointer<storage::SqlTable> table) const {
    return table->table_.layout_;
  }

  // Simulates the system shutting down and restarting
  void ShutdownAndRestartSystem() {
    // Simulate the system "shutting down". Guarantee persist of log records
    delete gc_thread_;
    StorageTestUtil::FullyPerformGC(gc_, log_manager_);
    log_manager_->PersistAndStop();

    // We now "boot up" up the system
    log_manager_->Start();
    gc_thread_ = new storage::GarbageCollectorThread(gc_, gc_period_);
  }

  void RunTest(const LargeSqlTableTestConfiguration &config) {
    // Run workload
    auto *tested = new LargeSqlTableTestObject(config, txn_manager_, catalog_, &block_store_, &generator_);
    tested->SimulateOltp(100, 4);

    ShutdownAndRestartSystem();

    // Instantiate recovery manager, and recover the tables.
    DiskLogProvider log_provider(LOG_FILE_NAME);
    RecoveryManager recovery_manager(&log_provider, common::ManagedPointer(recovery_catalog_), recovery_txn_manager_,
                                     recovery_deferred_action_manager_, common::ManagedPointer(&thread_registry_),
                                     &block_store_);
    recovery_manager.StartRecovery();
    recovery_manager.WaitForRecoveryToFinish();

    // Check we recovered all the original tables
    for (auto &database : tested->GetTables()) {
      auto database_oid = database.first;
      for (auto &table_oid : database.second) {
        // Get original sql table
        auto original_txn = txn_manager_->BeginTransaction();
        auto original_sql_table =
            catalog_->GetDatabaseCatalog(original_txn, database_oid)->GetTable(original_txn, table_oid);

        // Get Recovered table
        auto *recovery_txn = recovery_txn_manager_->BeginTransaction();
        auto db_catalog = recovery_catalog_->GetDatabaseCatalog(recovery_txn, database_oid);
        EXPECT_TRUE(db_catalog != nullptr);
        auto recovered_sql_table = db_catalog->GetTable(recovery_txn, table_oid);
        EXPECT_TRUE(recovered_sql_table != nullptr);

        EXPECT_TRUE(StorageTestUtil::SqlTableEqualDeep(
            original_sql_table->table_.layout_, original_sql_table, recovered_sql_table,
            tested->GetTupleSlotsForTable(database_oid, table_oid), recovery_manager.tuple_slot_map_, txn_manager_,
            recovery_txn_manager_));
        txn_manager_->Commit(original_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
        recovery_txn_manager_->Commit(recovery_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
      }
    }
    delete tested;
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
                                              .Build();
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
                                              .Build();
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
                                              .Build();
  RecoveryTests::RunTest(config);
}

// Tests that we correctly process records corresponding to a drop database command.
// NOLINTNEXTLINE
TEST_F(RecoveryTests, DropDatabaseTest) {
  std::string database_name = "testdb";

  // Create and drop the database
  auto *txn = txn_manager_->BeginTransaction();
  auto db_oid = CreateDatabase(txn, catalog_, database_name);
  DropDatabase(txn, catalog_, db_oid);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  ShutdownAndRestartSystem();

  // Instantiate recovery manager, and recover the catalog_->
  DiskLogProvider log_provider(LOG_FILE_NAME);
  RecoveryManager recovery_manager(&log_provider, common::ManagedPointer(recovery_catalog_), recovery_txn_manager_,
                                   recovery_deferred_action_manager_, common::ManagedPointer(&thread_registry_),
                                   &block_store_);
  recovery_manager.StartRecovery();
  recovery_manager.WaitForRecoveryToFinish();

  // Assert the database we deleted doesn't exist
  txn = recovery_txn_manager_->BeginTransaction();
  EXPECT_EQ(catalog::INVALID_DATABASE_OID, recovery_catalog_->GetDatabaseOid(txn, database_name));
  EXPECT_FALSE(recovery_catalog_->GetDatabaseCatalog(txn, db_oid));
  recovery_txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// Tests that we correctly process records corresponding to a drop table command.
// NOLINTNEXTLINE
TEST_F(RecoveryTests, DropTableTest) {
  std::string database_name = "testdb";
  auto namespace_oid = catalog::postgres::NAMESPACE_DEFAULT_NAMESPACE_OID;
  std::string table_name = "testtable";

  // Create database, table, then drop the table
  auto *txn = txn_manager_->BeginTransaction();
  auto db_oid = CreateDatabase(txn, catalog_, database_name);
  auto db_catalog = catalog_->GetDatabaseCatalog(txn, db_oid);
  auto table_oid = CreateTable(txn, db_catalog, namespace_oid, table_name);
  DropTable(txn, db_catalog, table_oid);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  ShutdownAndRestartSystem();

  // Instantiate recovery manager, and recover the catalog_->
  DiskLogProvider log_provider(LOG_FILE_NAME);
  RecoveryManager recovery_manager(&log_provider, common::ManagedPointer(recovery_catalog_), recovery_txn_manager_,
                                   recovery_deferred_action_manager_, common::ManagedPointer(&thread_registry_),
                                   &block_store_);
  recovery_manager.StartRecovery();
  recovery_manager.WaitForRecoveryToFinish();

  // Assert the database we created exists
  txn = recovery_txn_manager_->BeginTransaction();
  EXPECT_EQ(db_oid, recovery_catalog_->GetDatabaseOid(txn, database_name));
  db_catalog = recovery_catalog_->GetDatabaseCatalog(txn, db_oid);
  EXPECT_TRUE(db_catalog);

  // Assert the table we deleted doesn't exist
  EXPECT_EQ(catalog::INVALID_TABLE_OID, db_catalog->GetTableOid(txn, namespace_oid, table_name));
  EXPECT_FALSE(db_catalog->GetTable(txn, table_oid));
  recovery_txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// Tests that we correctly process records corresponding to a drop index command.
// NOLINTNEXTLINE
TEST_F(RecoveryTests, DropIndexTest) {
  std::string database_name = "testdb";
  auto namespace_oid = catalog::postgres::NAMESPACE_DEFAULT_NAMESPACE_OID;
  std::string table_name = "testtable";
  std::string index_name = "testindex";

  // Create database, table, index, then drop the index
  auto *txn = txn_manager_->BeginTransaction();
  auto db_oid = CreateDatabase(txn, catalog_, database_name);
  auto db_catalog = catalog_->GetDatabaseCatalog(txn, db_oid);
  auto table_oid = CreateTable(txn, db_catalog, namespace_oid, table_name);
  auto index_oid = CreateIndex(txn, db_catalog, namespace_oid, table_oid, index_name);
  DropIndex(txn, db_catalog, index_oid);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  ShutdownAndRestartSystem();

  // Instantiate recovery manager, and recover the catalog_->
  DiskLogProvider log_provider(LOG_FILE_NAME);
  RecoveryManager recovery_manager(&log_provider, common::ManagedPointer(recovery_catalog_), recovery_txn_manager_,
                                   recovery_deferred_action_manager_, common::ManagedPointer(&thread_registry_),
                                   &block_store_);
  recovery_manager.StartRecovery();
  recovery_manager.WaitForRecoveryToFinish();

  // Assert the database we created exists
  txn = recovery_txn_manager_->BeginTransaction();
  EXPECT_EQ(db_oid, recovery_catalog_->GetDatabaseOid(txn, database_name));
  db_catalog = recovery_catalog_->GetDatabaseCatalog(txn, db_oid);
  EXPECT_TRUE(db_catalog);

  // Assert the table we created exists
  EXPECT_EQ(table_oid, db_catalog->GetTableOid(txn, namespace_oid, table_name));
  EXPECT_TRUE(db_catalog->GetTable(txn, table_oid));

  // Assert the index we deleted doesn't exist
  EXPECT_EQ(0, db_catalog->GetIndexOids(txn, table_oid).size());
  EXPECT_FALSE(db_catalog->GetIndex(txn, index_oid));
  recovery_txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// Tests that we correctly process records corresponding to a drop namespace command.
// NOLINTNEXTLINE
TEST_F(RecoveryTests, DropNamespaceTest) {
  std::string database_name = "testdb";
  std::string namespace_name = "testnamespace";

  // Create database, namespace, then drop namespace
  auto *txn = txn_manager_->BeginTransaction();
  auto db_oid = CreateDatabase(txn, catalog_, database_name);
  auto db_catalog = catalog_->GetDatabaseCatalog(txn, db_oid);
  auto ns_oid = CreateNamespace(txn, db_catalog, namespace_name);
  DropNamespace(txn, db_catalog, ns_oid);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  ShutdownAndRestartSystem();

  // Instantiate recovery manager, and recover the catalog_->
  DiskLogProvider log_provider(LOG_FILE_NAME);
  RecoveryManager recovery_manager(&log_provider, common::ManagedPointer(recovery_catalog_), recovery_txn_manager_,
                                   recovery_deferred_action_manager_, common::ManagedPointer(&thread_registry_),
                                   &block_store_);
  recovery_manager.StartRecovery();
  recovery_manager.WaitForRecoveryToFinish();

  // Assert the database we created exists
  txn = recovery_txn_manager_->BeginTransaction();
  EXPECT_EQ(db_oid, recovery_catalog_->GetDatabaseOid(txn, database_name));
  db_catalog = recovery_catalog_->GetDatabaseCatalog(txn, db_oid);
  EXPECT_TRUE(db_catalog);

  // Assert the namespace we deleted doesn't exist
  EXPECT_EQ(catalog::INVALID_NAMESPACE_OID, db_catalog->GetNamespaceOid(txn, namespace_name));
  recovery_txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// Tests that we correctly process cascading deletes originating from a drop database command. This means cascading
// deletes of tables and indexes
// NOLINTNEXTLINE
TEST_F(RecoveryTests, DropDatabaseCascadeDeleteTest) {
  std::string database_name = "testdb";
  std::string namespace_name = "testnamespace";
  std::string table_name = "testtable";
  std::string index_name = "testindex";

  // Create a database, namespace, table, index, and then drop the database
  auto *txn = txn_manager_->BeginTransaction();
  auto db_oid = CreateDatabase(txn, catalog_, database_name);
  auto db_catalog = catalog_->GetDatabaseCatalog(txn, db_oid);
  auto ns_oid = CreateNamespace(txn, db_catalog, namespace_name);
  auto table_oid = CreateTable(txn, db_catalog, ns_oid, table_name);
  CreateIndex(txn, db_catalog, ns_oid, table_oid, index_name);
  DropDatabase(txn, catalog_, db_oid);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  ShutdownAndRestartSystem();

  // Instantiate recovery manager, and recover the catalog_->
  DiskLogProvider log_provider(LOG_FILE_NAME);
  RecoveryManager recovery_manager(&log_provider, common::ManagedPointer(recovery_catalog_), recovery_txn_manager_,
                                   recovery_deferred_action_manager_, common::ManagedPointer(&thread_registry_),
                                   &block_store_);
  recovery_manager.StartRecovery();
  recovery_manager.WaitForRecoveryToFinish();

  // Assert the database does not exist
  txn = recovery_txn_manager_->BeginTransaction();
  EXPECT_EQ(catalog::INVALID_DATABASE_OID, recovery_catalog_->GetDatabaseOid(txn, database_name));
  EXPECT_FALSE(recovery_catalog_->GetDatabaseCatalog(txn, db_oid));
  recovery_txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// Tests that we correctly handle changes for transactions that never flushed a commit record. This is likely to happen
// during crash situations.
// NOLINTNEXTLINE
TEST_F(RecoveryTests, UnrecoverableTransactionsTest) {
  std::string database_name = "testdb";

  // Create a database and commit, we should see this one after recovery
  auto *txn = txn_manager_->BeginTransaction();
  auto db_oid = CreateDatabase(txn, catalog_, database_name);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // We insert a sleep in here so that GC has the opportunity to clean up the previous txn. Otherwise, the next txn will
  // prevent it from doing so since it never finishes, and by deleting the gc thread during "shutdown", the previous txn
  // will never get cleaned up.
  std::this_thread::sleep_for(5 * gc_period_);

  // Create a ton of databases to make a transaction flush redo record buffers. In theory we could do any change, but a
  // create database call will generate a ton of records. Importantly, we don't commit the txn.
  std::vector<catalog::db_oid_t> unrecoverable_databases;
  auto *unrecoverable_txn = txn_manager_->BeginTransaction();
  int db_idx = 0;
  while (!GetRedoBuffer(unrecoverable_txn).HasFlushed()) {
    unrecoverable_databases.push_back(CreateDatabase(unrecoverable_txn, catalog_, std::to_string(db_idx)));
    db_idx++;
  }

  // Simulate the system "crashing" (i.e. unrecoverable_txn has not been committed). We guarantee persist of log records
  // because the purpose of the test is how we handle these unrecoverable records showing up during recovery
  ShutdownAndRestartSystem();

  // Instantiate recovery manager, and recover the catalog_->
  DiskLogProvider log_provider(LOG_FILE_NAME);
  RecoveryManager recovery_manager(&log_provider, common::ManagedPointer(recovery_catalog_), recovery_txn_manager_,
                                   recovery_deferred_action_manager_, common::ManagedPointer(&thread_registry_),
                                   &block_store_);
  recovery_manager.StartRecovery();
  recovery_manager.WaitForRecoveryToFinish();

  // Assert the database creation we committed does exist
  txn = recovery_txn_manager_->BeginTransaction();
  EXPECT_EQ(db_oid, recovery_catalog_->GetDatabaseOid(txn, database_name));
  EXPECT_TRUE(recovery_catalog_->GetDatabaseCatalog(txn, db_oid));

  // Assert that none of the unrecoverable databases exist:
  for (int i = 0; i < db_idx; i++) {
    EXPECT_EQ(catalog::INVALID_DATABASE_OID, recovery_catalog_->GetDatabaseOid(txn, std::to_string(i)));
    EXPECT_FALSE(recovery_catalog_->GetDatabaseCatalog(txn, unrecoverable_databases[i]));
  }
  recovery_txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Finally abort the unrecoverable_txn so GC can clean it up
  txn_manager_->Abort(unrecoverable_txn);
}

// Tests we correct order transactions and execute GC when concurrent transactions make DDL changes to the catalog
// The transaction schedule for the following test is:
//           Txn #0     |          Txn #1          |      Txn #2     |
//    ----------------------------------------------------------------
//    BEGIN             |                          |
//    CREATE DB testdb; |                          |
//    COMMIT            |                          |
//                      | BEGIN                    |
//                      |                          | BEGIN
//                      |                          | DROP DB testdb
//                      |                          | COMMIT
//                      |                          |
//                      | CREATE TABLE testdb.foo; |
//                      | COMMIT                   |
//    -----------------------------------------------------------------
// Under snapshot isolation, all these transactions should succeed. At the time of recovery though, we want to ensure
// that even though the logs of txn #2 may appear before the logs of txn #1, we dont drop the database before txn #1 is
// able to create a table.
// NOLINTNEXTLINE
TEST_F(RecoveryTests, ConcurrentCatalogDDLChangesTest) {
  std::string database_name = "testdb";
  auto namespace_oid = catalog::postgres::NAMESPACE_DEFAULT_NAMESPACE_OID;
  std::string table_name = "foo";

  // Begin T0, create database, create table foo, and commit
  auto *txn0 = txn_manager_->BeginTransaction();
  auto db_oid = CreateDatabase(txn0, catalog_, database_name);
  txn_manager_->Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Begin T1
  auto txn1 = txn_manager_->BeginTransaction();

  // Begin T2, drop testdb, and commit
  auto txn2 = txn_manager_->BeginTransaction();
  DropDatabase(txn2, catalog_, db_oid);
  txn_manager_->Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);

  // With T1, create a table in testdb and commit. Even though T2 dropped testdb, this operation should still succeed
  // because T1 got a snapshot before T2
  auto db_catalog = catalog_->GetDatabaseCatalog(txn1, db_oid);
  CreateTable(txn1, db_catalog, namespace_oid, table_name);
  txn_manager_->Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

  ShutdownAndRestartSystem();

  // Instantiate recovery manager, and recover the catalog_->
  DiskLogProvider log_provider(LOG_FILE_NAME);
  RecoveryManager recovery_manager(&log_provider, common::ManagedPointer(recovery_catalog_), recovery_txn_manager_,
                                   recovery_deferred_action_manager_, common::ManagedPointer(&thread_registry_),
                                   &block_store_);
  recovery_manager.StartRecovery();
  recovery_manager.WaitForRecoveryToFinish();

  // Assert the database we created does not exists
  auto txn = recovery_txn_manager_->BeginTransaction();
  EXPECT_EQ(catalog::INVALID_DATABASE_OID, recovery_catalog_->GetDatabaseOid(txn, database_name));
  EXPECT_FALSE(recovery_catalog_->GetDatabaseCatalog(txn, db_oid));
  recovery_txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// Tests we correct order transactions and execute GC when concurrent transactions make inserts and DDL changes to
// tables The transaction schedule for the following test is:
//           Txn #0     |          Txn #1          |      Txn #2     |
//    ----------------------------------------------------------------
//    BEGIN             |                          |
//    CREATE DB testdb  |                          |
//    CREATE TABLE foo  |                          |
//    COMMIT            |                          |
//                      | BEGIN                    |
//                      |                          | BEGIN
//                      |                          | DROP table foo
//                      |                          | COMMIT
//                      |                          |
//                      | insert a into foo        |
//                      | COMMIT                   |
//    -----------------------------------------------------------------
// Under snapshot isolation, all these transactions should succeed. At the time of recovery though, we want to ensure
// that even though the logs of txn #2 may appear before the logs of txn #1, we dont drop foo before txn #1 is
// able to insert into it.
// NOLINTNEXTLINE
TEST_F(RecoveryTests, ConcurrentDDLChangesTest) {
  std::string database_name = "testdb";
  auto namespace_oid = catalog::postgres::NAMESPACE_DEFAULT_NAMESPACE_OID;
  std::string table_name = "foo";

  // Begin T0, create database, create table foo, and commit
  auto *txn0 = txn_manager_->BeginTransaction();
  auto db_oid = CreateDatabase(txn0, catalog_, database_name);
  auto db_catalog = catalog_->GetDatabaseCatalog(txn0, db_oid);
  auto table_oid = CreateTable(txn0, db_catalog, namespace_oid, table_name);
  txn_manager_->Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Begin T1
  auto txn1 = txn_manager_->BeginTransaction();

  // Begin T2, drop foo, and commit
  auto txn2 = txn_manager_->BeginTransaction();
  db_catalog = catalog_->GetDatabaseCatalog(txn2, db_oid);
  DropTable(txn2, db_catalog, table_oid);
  txn_manager_->Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);

  // With T1, insert into foo and commit. Even though T2 dropped foo, this operation should still succeed
  // because T1 got a snapshot before T2
  db_catalog = catalog_->GetDatabaseCatalog(txn1, db_oid);
  auto table_ptr = db_catalog->GetTable(txn1, table_oid);
  const auto &schema = db_catalog->GetSchema(txn1, table_oid);
  EXPECT_EQ(1, schema.GetColumns().size());
  EXPECT_EQ(type::TypeId::INTEGER, schema.GetColumn(0).Type());
  auto initializer = table_ptr->InitializerForProjectedRow({schema.GetColumn(0).Oid()});
  auto *redo_record = txn1->StageWrite(db_oid, table_oid, initializer);
  *reinterpret_cast<int32_t *>(redo_record->Delta()->AccessForceNotNull(0)) = 0;
  table_ptr->Insert(txn1, redo_record);
  txn_manager_->Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

  ShutdownAndRestartSystem();

  // Instantiate recovery manager, and recover the catalog
  DiskLogProvider log_provider(LOG_FILE_NAME);
  RecoveryManager recovery_manager(&log_provider, common::ManagedPointer(recovery_catalog_), recovery_txn_manager_,
                                   recovery_deferred_action_manager_, common::ManagedPointer(&thread_registry_),
                                   &block_store_);
  recovery_manager.StartRecovery();
  recovery_manager.WaitForRecoveryToFinish();

  // Assert the database we created does not exists
  auto txn = recovery_txn_manager_->BeginTransaction();
  EXPECT_EQ(db_oid, recovery_catalog_->GetDatabaseOid(txn, database_name));
  EXPECT_TRUE(recovery_catalog_->GetDatabaseCatalog(txn, db_oid));

  // Assert the table we deleted doesn't exist
  EXPECT_EQ(catalog::INVALID_TABLE_OID, db_catalog->GetTableOid(txn, namespace_oid, table_name));
  EXPECT_FALSE(db_catalog->GetTable(txn, table_oid));
  recovery_txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// Tests that we can recover from a previous instance of recovery. We do this by recovering a workload, and then
// recovering from the logs generated by the original workload's recovery.
// NOLINTNEXTLINE
TEST_F(RecoveryTests, DoubleRecoveryTest) {
  std::string secondary_log_file = "test2.log";
  unlink(secondary_log_file.c_str());
  LargeSqlTableTestConfiguration config = LargeSqlTableTestConfiguration::Builder()
                                              .SetNumDatabases(1)
                                              .SetNumTables(1)
                                              .SetMaxColumns(5)
                                              .SetInitialTableSize(1000)
                                              .SetTxnLength(5)
                                              .SetInsertUpdateSelectDeleteRatio({0.2, 0.5, 0.2, 0.1})
                                              .SetVarlenAllowed(true)
                                              .Build();
  auto *tested = new LargeSqlTableTestObject(config, txn_manager_, catalog_, &block_store_, &generator_);

  // Run workload
  tested->SimulateOltp(100, 4);

  ShutdownAndRestartSystem();

  // We create a new log manager to log the changes replayed during recovery
  LogManager secondary_log_manager(secondary_log_file, num_log_buffers_, log_serialization_interval_,
                                   log_persist_interval_, log_persist_threshold_, &buffer_pool_,
                                   common::ManagedPointer(&thread_registry_));
  secondary_log_manager.Start();

  // Override the recovery txn manager to now log out
  recovery_catalog_->TearDown();
  delete recovery_gc_thread_;
  StorageTestUtil::FullyPerformGC(recovery_gc_, DISABLED);
  delete recovery_gc_;
  delete recovery_catalog_;
  delete recovery_txn_manager_;
  recovery_txn_manager_ = new transaction::TransactionManager(
      recovery_timestamp_manager_, recovery_deferred_action_manager_, &buffer_pool_, true, &secondary_log_manager);
  recovery_catalog_ = new catalog::Catalog(recovery_txn_manager_, &block_store_);
  recovery_gc_ = new storage::GarbageCollector(recovery_timestamp_manager_, recovery_deferred_action_manager_,
                                               recovery_txn_manager_, DISABLED);
  recovery_gc_thread_ = new storage::GarbageCollectorThread(recovery_gc_, gc_period_);

  //--------------------------------
  // Do recovery for the first time
  //--------------------------------

  // Instantiate recovery manager, and recover the tables.
  DiskLogProvider log_provider(LOG_FILE_NAME);
  RecoveryManager recovery_manager(&log_provider, common::ManagedPointer(recovery_catalog_), recovery_txn_manager_,
                                   recovery_deferred_action_manager_, common::ManagedPointer(&thread_registry_),
                                   &block_store_);
  recovery_manager.StartRecovery();
  recovery_manager.WaitForRecoveryToFinish();

  // Check we recovered all the original tables
  for (auto &database : tested->GetTables()) {
    auto database_oid = database.first;
    for (auto &table_oid : database.second) {
      // Get original sql table
      auto original_txn = txn_manager_->BeginTransaction();
      auto original_sql_table =
          catalog_->GetDatabaseCatalog(original_txn, database_oid)->GetTable(original_txn, table_oid);

      // Get Recovered table
      auto *recovery_txn = recovery_txn_manager_->BeginTransaction();
      auto db_catalog = recovery_catalog_->GetDatabaseCatalog(recovery_txn, database_oid);
      EXPECT_TRUE(db_catalog != nullptr);
      auto recovered_sql_table = db_catalog->GetTable(recovery_txn, table_oid);
      EXPECT_TRUE(recovered_sql_table != nullptr);

      EXPECT_TRUE(StorageTestUtil::SqlTableEqualDeep(
          GetBlockLayout(original_sql_table), original_sql_table, recovered_sql_table,
          tested->GetTupleSlotsForTable(database_oid, table_oid), recovery_manager.tuple_slot_map_, txn_manager_,
          recovery_txn_manager_));
      txn_manager_->Commit(original_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
      recovery_txn_manager_->Commit(recovery_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
    }
  }

  // Contrary to other tests, we clean up the recovery catalog and gc thread here because the secondary_log_manager is a
  // local object. Setting the appropriate variables to nullptr will allow the TearDown code to run normally.
  recovery_catalog_->TearDown();
  delete recovery_gc_thread_;
  StorageTestUtil::FullyPerformGC(recovery_gc_, &secondary_log_manager);
  delete recovery_catalog_;
  recovery_gc_thread_ = nullptr;
  recovery_catalog_ = nullptr;
  secondary_log_manager.PersistAndStop();

  log_manager_->PersistAndStop();
  //-----------------------------------------
  // Now recover based off the last recovery
  //-----------------------------------------
  log_manager_->Start();

  // Create a new txn manager with logging disabled
  transaction::TimestampManager secondary_recovery_timestamp_manager;
  transaction::DeferredActionManager secondary_recovery_deferred_action_manager{&secondary_recovery_timestamp_manager};
  transaction::TransactionManager secondary_recovery_txn_manager{&secondary_recovery_timestamp_manager,
                                                                 &secondary_recovery_deferred_action_manager,
                                                                 &buffer_pool_, true, DISABLED};
  storage::GarbageCollector secondary_recovery_gc{&secondary_recovery_timestamp_manager,
                                                  &secondary_recovery_deferred_action_manager,
                                                  &secondary_recovery_txn_manager, DISABLED};
  auto secondary_recovery_gc_thread = new storage::GarbageCollectorThread(&secondary_recovery_gc, gc_period_);

  // Create a new catalog for this second recovery
  catalog::Catalog secondary_recovery_catalog{&secondary_recovery_txn_manager, &block_store_};

  // Instantiate a new recovery manager, and recover the tables.
  DiskLogProvider secondary_log_provider(secondary_log_file);
  RecoveryManager secondary_recovery_manager(
      &secondary_log_provider, common::ManagedPointer(&secondary_recovery_catalog), &secondary_recovery_txn_manager,
      &secondary_recovery_deferred_action_manager, common::ManagedPointer(&thread_registry_), &block_store_);
  secondary_recovery_manager.StartRecovery();
  secondary_recovery_manager.WaitForRecoveryToFinish();

  // Maps from tuple slots in original tables to tuple slots in tables after second recovery
  std::unordered_map<TupleSlot, TupleSlot> new_tuple_slot_map;
  for (const auto &slot_pair : recovery_manager.tuple_slot_map_) {
    new_tuple_slot_map[slot_pair.first] = secondary_recovery_manager.tuple_slot_map_[slot_pair.second];
  }

  // Check we recovered all the original tables
  for (auto &database : tested->GetTables()) {
    auto database_oid = database.first;
    for (auto &table_oid : database.second) {
      // Get original sql table
      auto original_txn = txn_manager_->BeginTransaction();
      auto original_sql_table =
          catalog_->GetDatabaseCatalog(original_txn, database_oid)->GetTable(original_txn, table_oid);

      // Get Recovered table
      auto *recovery_txn = secondary_recovery_txn_manager.BeginTransaction();
      auto db_catalog = secondary_recovery_catalog.GetDatabaseCatalog(recovery_txn, database_oid);
      EXPECT_TRUE(db_catalog != nullptr);
      auto recovered_sql_table = db_catalog->GetTable(recovery_txn, table_oid);
      EXPECT_TRUE(recovered_sql_table != nullptr);

      EXPECT_TRUE(StorageTestUtil::SqlTableEqualDeep(
          GetBlockLayout(original_sql_table), original_sql_table, recovered_sql_table,
          tested->GetTupleSlotsForTable(database_oid, table_oid), new_tuple_slot_map, txn_manager_,
          &secondary_recovery_txn_manager));
      txn_manager_->Commit(original_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
      secondary_recovery_txn_manager.Commit(recovery_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
    }
  }
  // Clean up test object and recovered catalogs
  delete tested;
  secondary_recovery_catalog.TearDown();
  delete secondary_recovery_gc_thread;
  StorageTestUtil::FullyPerformGC(&secondary_recovery_gc, DISABLED);
  unlink(secondary_log_file.c_str());
}
}  // namespace terrier::storage
