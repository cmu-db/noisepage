#include <memory>
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
#include "test_util/catalog_test_util.h"
#include "test_util/sql_table_test_util.h"
#include "test_util/storage_test_util.h"
#include "test_util/test_harness.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"

// Make sure that if you create additional files, you call unlink on them after the test finishes. Otherwise, repeated
// executions will read old test's data, and the cause of the errors will be hard to identify. Trust me it will drive
// you nuts...
#define RECOVERY_TEST_LOG_FILE_NAME "./test_recovery_test.log"

namespace noisepage::storage {
class RecoveryTests : public TerrierTest {
 protected:
  std::default_random_engine generator_;

  // Original Components
  std::unique_ptr<DBMain> db_main_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  common::ManagedPointer<storage::LogManager> log_manager_;
  common::ManagedPointer<storage::BlockStore> block_store_;
  common::ManagedPointer<catalog::Catalog> catalog_;

  // Recovery Components
  std::unique_ptr<DBMain> recovery_db_main_;
  common::ManagedPointer<transaction::TransactionManager> recovery_txn_manager_;
  common::ManagedPointer<transaction::DeferredActionManager> recovery_deferred_action_manager_;
  common::ManagedPointer<storage::BlockStore> recovery_block_store_;
  common::ManagedPointer<catalog::Catalog> recovery_catalog_;
  common::ManagedPointer<common::DedicatedThreadRegistry> recovery_thread_registry_;

  void SetUp() override {
    // Unlink log file incase one exists from previous test iteration
    unlink(RECOVERY_TEST_LOG_FILE_NAME);

    db_main_ = noisepage::DBMain::Builder()
                   .SetWalFilePath(RECOVERY_TEST_LOG_FILE_NAME)
                   .SetUseLogging(true)
                   .SetUseGC(true)
                   .SetUseGCThread(true)
                   .SetUseCatalog(true)
                   .Build();
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
    log_manager_ = db_main_->GetLogManager();
    block_store_ = db_main_->GetStorageLayer()->GetBlockStore();
    catalog_ = db_main_->GetCatalogLayer()->GetCatalog();

    recovery_db_main_ = noisepage::DBMain::Builder()
                            .SetUseThreadRegistry(true)
                            .SetUseGC(true)
                            .SetUseGCThread(true)
                            .SetUseCatalog(true)
                            .SetCreateDefaultDatabase(false)
                            .Build();
    recovery_txn_manager_ = recovery_db_main_->GetTransactionLayer()->GetTransactionManager();
    recovery_deferred_action_manager_ = recovery_db_main_->GetTransactionLayer()->GetDeferredActionManager();
    recovery_block_store_ = recovery_db_main_->GetStorageLayer()->GetBlockStore();
    recovery_catalog_ = recovery_db_main_->GetCatalogLayer()->GetCatalog();
    recovery_thread_registry_ = recovery_db_main_->GetThreadRegistry();
  }

  void TearDown() override {
    // Delete log file
    unlink(RECOVERY_TEST_LOG_FILE_NAME);
  }

  catalog::IndexSchema DummyIndexSchema() {
    std::vector<catalog::IndexSchema::Column> keycols;
    keycols.emplace_back(
        "", type::TypeId::INTEGER, false,
        parser::ColumnValueExpression(catalog::db_oid_t(0), catalog::table_oid_t(0), catalog::col_oid_t(1)));
    StorageTestUtil::ForceOid(&(keycols[0]), catalog::indexkeycol_oid_t(1));
    return catalog::IndexSchema(keycols, storage::index::IndexType::BWTREE, true, true, false, true);
  }

  catalog::db_oid_t CreateDatabase(transaction::TransactionContext *txn,
                                   common::ManagedPointer<catalog::Catalog> catalog, const std::string &database_name) {
    auto db_oid = catalog->CreateDatabase(common::ManagedPointer(txn), database_name, true /* bootstrap */);
    EXPECT_TRUE(db_oid != catalog::INVALID_DATABASE_OID);
    return db_oid;
  }

  void DropDatabase(transaction::TransactionContext *txn, common::ManagedPointer<catalog::Catalog> catalog,
                    const catalog::db_oid_t db_oid) {
    EXPECT_TRUE(catalog->DeleteDatabase(common::ManagedPointer(txn), db_oid));
    EXPECT_FALSE(catalog->GetDatabaseCatalog(common::ManagedPointer(txn), db_oid));
  }

  catalog::table_oid_t CreateTable(transaction::TransactionContext *txn,
                                   common::ManagedPointer<catalog::DatabaseCatalog> db_catalog,
                                   const catalog::namespace_oid_t ns_oid, const std::string &table_name) {
    auto col = catalog::Schema::Column("attribute", type::TypeId::INTEGER, false,
                                       parser::ConstantValueExpression(type::TypeId::INTEGER));
    auto table_schema = catalog::Schema(std::vector<catalog::Schema::Column>({col}));
    auto table_oid = db_catalog->CreateTable(common::ManagedPointer(txn), ns_oid, table_name, table_schema);
    EXPECT_TRUE(table_oid != catalog::INVALID_TABLE_OID);
    const auto catalog_schema = db_catalog->GetSchema(common::ManagedPointer(txn), table_oid);
    auto *table_ptr = new storage::SqlTable(block_store_, catalog_schema);
    EXPECT_TRUE(db_catalog->SetTablePointer(common::ManagedPointer(txn), table_oid, table_ptr));
    return table_oid;
  }

  void DropTable(transaction::TransactionContext *txn, common::ManagedPointer<catalog::DatabaseCatalog> db_catalog,
                 const catalog::table_oid_t table_oid) {
    EXPECT_TRUE(db_catalog->DeleteTable(common::ManagedPointer(txn), table_oid));
  }

  catalog::index_oid_t CreateIndex(transaction::TransactionContext *txn,
                                   common::ManagedPointer<catalog::DatabaseCatalog> db_catalog,
                                   const catalog::namespace_oid_t ns_oid, const catalog::table_oid_t table_oid,
                                   const std::string &index_name) {
    auto index_schema = DummyIndexSchema();
    auto index_oid = db_catalog->CreateIndex(common::ManagedPointer(txn), ns_oid, index_name, table_oid, index_schema);
    EXPECT_TRUE(index_oid != catalog::INVALID_INDEX_OID);
    auto *index_ptr = storage::index::IndexBuilder().SetKeySchema(index_schema).Build();
    EXPECT_TRUE(db_catalog->SetIndexPointer(common::ManagedPointer(txn), index_oid, index_ptr));
    return index_oid;
  }

  void DropIndex(transaction::TransactionContext *txn, common::ManagedPointer<catalog::DatabaseCatalog> db_catalog,
                 const catalog::index_oid_t index_oid) {
    EXPECT_TRUE(db_catalog->DeleteIndex(common::ManagedPointer(txn), index_oid));
  }

  catalog::namespace_oid_t CreateNamespace(transaction::TransactionContext *txn,
                                           common::ManagedPointer<catalog::DatabaseCatalog> db_catalog,
                                           const std::string &namespace_name) {
    auto namespace_oid = db_catalog->CreateNamespace(common::ManagedPointer(txn), namespace_name);
    EXPECT_TRUE(namespace_oid != catalog::INVALID_NAMESPACE_OID);
    return namespace_oid;
  }

  void DropNamespace(transaction::TransactionContext *txn, common::ManagedPointer<catalog::DatabaseCatalog> db_catalog,
                     const catalog::namespace_oid_t ns_oid) {
    EXPECT_TRUE(db_catalog->DeleteNamespace(common::ManagedPointer(txn), ns_oid));
  }

  storage::RedoBuffer &GetRedoBuffer(transaction::TransactionContext *txn) { return txn->redo_buffer_; }

  storage::BlockLayout &GetBlockLayout(common::ManagedPointer<storage::SqlTable> table) const {
    return table->table_.layout_;
  }

  // Simulates the system shutting down and restarting
  void ShutdownAndRestartSystem() {
    // Simulate the system "shutting down". Guarantee persist of log records
    db_main_->GetGarbageCollectorThread()->StopGC();
    db_main_->GetTransactionLayer()->GetDeferredActionManager()->FullyPerformGC(
        db_main_->GetStorageLayer()->GetGarbageCollector(), log_manager_);
    log_manager_->PersistAndStop();

    // We now "boot up" up the system
    log_manager_->Start();
    db_main_->GetGarbageCollectorThread()->StartGC();
  }

  // Most tests do a single recovery pass into the recovery DBMain
  void SingleRecovery() {
    DiskLogProvider log_provider(RECOVERY_TEST_LOG_FILE_NAME);
    RecoveryManager recovery_manager{common::ManagedPointer<AbstractLogProvider>(&log_provider),
                                     recovery_catalog_,
                                     recovery_txn_manager_,
                                     recovery_deferred_action_manager_,
                                     DISABLED,
                                     recovery_thread_registry_,
                                     recovery_block_store_};
    recovery_manager.StartRecovery();
    recovery_manager.WaitForRecoveryToFinish();
  }

  void RunTest(const LargeSqlTableTestConfiguration &config) {
    // Run workload
    auto *tested =
        new LargeSqlTableTestObject(config, txn_manager_.Get(), catalog_.Get(), block_store_.Get(), &generator_);
    tested->SimulateOltp(100, 4);

    ShutdownAndRestartSystem();

    // Instantiate recovery manager, and recover the tables.
    DiskLogProvider log_provider{RECOVERY_TEST_LOG_FILE_NAME};
    RecoveryManager recovery_manager{common::ManagedPointer<AbstractLogProvider>(&log_provider),
                                     recovery_catalog_,
                                     recovery_txn_manager_,
                                     recovery_deferred_action_manager_,
                                     DISABLED,
                                     recovery_thread_registry_,
                                     recovery_block_store_};
    recovery_manager.StartRecovery();
    recovery_manager.WaitForRecoveryToFinish();

    // Check we recovered all the original tables
    for (auto &database : tested->GetTables()) {
      auto database_oid = database.first;
      for (auto &table_oid : database.second) {
        // Get original sql table
        auto original_txn = txn_manager_->BeginTransaction();
        auto original_sql_table = catalog_->GetDatabaseCatalog(common::ManagedPointer(original_txn), database_oid)
                                      ->GetTable(common::ManagedPointer(original_txn), table_oid);

        // Get Recovered table
        auto *recovery_txn = recovery_txn_manager_->BeginTransaction();
        auto db_catalog = recovery_catalog_->GetDatabaseCatalog(common::ManagedPointer(recovery_txn), database_oid);
        EXPECT_TRUE(db_catalog != nullptr);
        auto recovered_sql_table = db_catalog->GetTable(common::ManagedPointer(recovery_txn), table_oid);
        EXPECT_TRUE(recovered_sql_table != nullptr);

        EXPECT_TRUE(StorageTestUtil::SqlTableEqualDeep(
            original_sql_table->table_.layout_, original_sql_table, recovered_sql_table,
            tested->GetTupleSlotsForTable(database_oid, table_oid), recovery_manager.tuple_slot_map_,
            txn_manager_.Get(), recovery_txn_manager_.Get()));
        txn_manager_->Commit(original_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
        recovery_txn_manager_->Commit(recovery_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
      }
    }
    // the table can't be freed until after all GC on it is guaranteed to be done. The easy way to do that is to use a
    // DeferredAction
    db_main_->GetTransactionLayer()->GetDeferredActionManager()->RegisterDeferredAction([=]() { delete tested; });
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

  // Instantiate recovery manager, and recover the catalog
  SingleRecovery();

  // Assert the database we deleted doesn't exist
  txn = recovery_txn_manager_->BeginTransaction();
  EXPECT_EQ(catalog::INVALID_DATABASE_OID,
            recovery_catalog_->GetDatabaseOid(common::ManagedPointer(txn), database_name));
  EXPECT_FALSE(recovery_catalog_->GetDatabaseCatalog(common::ManagedPointer(txn), db_oid));
  recovery_txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// Tests that we correctly process records corresponding to a drop table command.
// NOLINTNEXTLINE
TEST_F(RecoveryTests, DropTableTest) {
  std::string database_name = "testdb";
  auto namespace_oid = catalog::postgres::PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID;
  std::string table_name = "testtable";

  // Create database, table, then drop the table
  auto *txn = txn_manager_->BeginTransaction();
  auto db_oid = CreateDatabase(txn, catalog_, database_name);
  auto db_catalog = catalog_->GetDatabaseCatalog(common::ManagedPointer(txn), db_oid);
  auto table_oid = CreateTable(txn, db_catalog, namespace_oid, table_name);
  DropTable(txn, db_catalog, table_oid);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  ShutdownAndRestartSystem();

  // Instantiate recovery manager, and recover the catalog
  SingleRecovery();

  // Assert the database we created exists
  txn = recovery_txn_manager_->BeginTransaction();
  EXPECT_EQ(db_oid, recovery_catalog_->GetDatabaseOid(common::ManagedPointer(txn), database_name));
  db_catalog = recovery_catalog_->GetDatabaseCatalog(common::ManagedPointer(txn), db_oid);
  EXPECT_TRUE(db_catalog);

  // Assert the table we deleted doesn't exist
  EXPECT_EQ(catalog::INVALID_TABLE_OID,
            db_catalog->GetTableOid(common::ManagedPointer(txn), namespace_oid, table_name));
  recovery_txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// Tests that we correctly process records corresponding to a drop index command.
// NOLINTNEXTLINE
TEST_F(RecoveryTests, DropIndexTest) {
  std::string database_name = "testdb";
  auto namespace_oid = catalog::postgres::PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID;
  std::string table_name = "testtable";
  std::string index_name = "testindex";

  // Create database, table, index, then drop the index
  auto *txn = txn_manager_->BeginTransaction();
  auto db_oid = CreateDatabase(txn, catalog_, database_name);
  auto db_catalog = catalog_->GetDatabaseCatalog(common::ManagedPointer(txn), db_oid);
  auto table_oid = CreateTable(txn, db_catalog, namespace_oid, table_name);
  auto index_oid = CreateIndex(txn, db_catalog, namespace_oid, table_oid, index_name);
  DropIndex(txn, db_catalog, index_oid);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  ShutdownAndRestartSystem();

  // Instantiate recovery manager, and recover the catalog
  SingleRecovery();

  // Assert the database we created exists
  txn = recovery_txn_manager_->BeginTransaction();
  EXPECT_EQ(db_oid, recovery_catalog_->GetDatabaseOid(common::ManagedPointer(txn), database_name));
  db_catalog = recovery_catalog_->GetDatabaseCatalog(common::ManagedPointer(txn), db_oid);
  EXPECT_TRUE(db_catalog);

  // Assert the table we created exists
  EXPECT_EQ(table_oid, db_catalog->GetTableOid(common::ManagedPointer(txn), namespace_oid, table_name));
  EXPECT_TRUE(db_catalog->GetTable(common::ManagedPointer(txn), table_oid));

  // Assert the index we deleted doesn't exist
  EXPECT_EQ(0, db_catalog->GetIndexOids(common::ManagedPointer(txn), table_oid).size());
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
  auto db_catalog = catalog_->GetDatabaseCatalog(common::ManagedPointer(txn), db_oid);
  auto ns_oid = CreateNamespace(txn, db_catalog, namespace_name);
  DropNamespace(txn, db_catalog, ns_oid);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  ShutdownAndRestartSystem();

  // Instantiate recovery manager, and recover the catalog
  SingleRecovery();

  // Assert the database we created exists
  txn = recovery_txn_manager_->BeginTransaction();
  EXPECT_EQ(db_oid, recovery_catalog_->GetDatabaseOid(common::ManagedPointer(txn), database_name));
  db_catalog = recovery_catalog_->GetDatabaseCatalog(common::ManagedPointer(txn), db_oid);
  EXPECT_TRUE(db_catalog);

  // Assert the namespace we deleted doesn't exist
  EXPECT_EQ(catalog::INVALID_NAMESPACE_OID, db_catalog->GetNamespaceOid(common::ManagedPointer(txn), namespace_name));
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
  auto db_catalog = catalog_->GetDatabaseCatalog(common::ManagedPointer(txn), db_oid);
  auto ns_oid = CreateNamespace(txn, db_catalog, namespace_name);
  auto table_oid = CreateTable(txn, db_catalog, ns_oid, table_name);
  CreateIndex(txn, db_catalog, ns_oid, table_oid, index_name);
  DropDatabase(txn, catalog_, db_oid);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  ShutdownAndRestartSystem();

  // Instantiate recovery manager, and recover the catalog
  SingleRecovery();

  // Assert the database does not exist
  txn = recovery_txn_manager_->BeginTransaction();
  EXPECT_EQ(catalog::INVALID_DATABASE_OID,
            recovery_catalog_->GetDatabaseOid(common::ManagedPointer(txn), database_name));
  EXPECT_FALSE(recovery_catalog_->GetDatabaseCatalog(common::ManagedPointer(txn), db_oid));
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

  // We insert a GC restart here so it has the opportunity to clean up the previous txn. Otherwise, the next txn will
  // prevent it from doing so since it never finishes, and by deleting the gc thread during "shutdown", the previous txn
  // will never get cleaned up.
  db_main_->GetGarbageCollectorThread()->StopGC();
  db_main_->GetGarbageCollectorThread()->StartGC();

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

  // Instantiate recovery manager, and recover the catalog
  SingleRecovery();

  // Assert the database creation we committed does exist
  txn = recovery_txn_manager_->BeginTransaction();
  EXPECT_EQ(db_oid, recovery_catalog_->GetDatabaseOid(common::ManagedPointer(txn), database_name));
  EXPECT_TRUE(recovery_catalog_->GetDatabaseCatalog(common::ManagedPointer(txn), db_oid));

  // Assert that none of the unrecoverable databases exist:
  for (int i = 0; i < db_idx; i++) {
    EXPECT_EQ(catalog::INVALID_DATABASE_OID,
              recovery_catalog_->GetDatabaseOid(common::ManagedPointer(txn), std::to_string(i)));
    EXPECT_FALSE(recovery_catalog_->GetDatabaseCatalog(common::ManagedPointer(txn), unrecoverable_databases[i]));
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
  auto namespace_oid = catalog::postgres::PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID;
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
  auto db_catalog = catalog_->GetDatabaseCatalog(common::ManagedPointer(txn1), db_oid);
  CreateTable(txn1, db_catalog, namespace_oid, table_name);
  txn_manager_->Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

  ShutdownAndRestartSystem();

  // Instantiate recovery manager, and recover the catalog
  SingleRecovery();

  // Assert the database we created does not exists
  auto txn = recovery_txn_manager_->BeginTransaction();
  EXPECT_EQ(catalog::INVALID_DATABASE_OID,
            recovery_catalog_->GetDatabaseOid(common::ManagedPointer(txn), database_name));
  EXPECT_FALSE(recovery_catalog_->GetDatabaseCatalog(common::ManagedPointer(txn), db_oid));
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
  auto namespace_oid = catalog::postgres::PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID;
  std::string table_name = "foo";

  // Begin T0, create database, create table foo, and commit
  auto *txn0 = txn_manager_->BeginTransaction();
  auto db_oid = CreateDatabase(txn0, catalog_, database_name);
  auto db_catalog = catalog_->GetDatabaseCatalog(common::ManagedPointer(txn0), db_oid);
  auto table_oid = CreateTable(txn0, db_catalog, namespace_oid, table_name);
  txn_manager_->Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Begin T1
  auto txn1 = txn_manager_->BeginTransaction();

  // Begin T2, drop foo, and commit
  auto txn2 = txn_manager_->BeginTransaction();
  db_catalog = catalog_->GetDatabaseCatalog(common::ManagedPointer(txn2), db_oid);
  DropTable(txn2, db_catalog, table_oid);
  txn_manager_->Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);

  // With T1, insert into foo and commit. Even though T2 dropped foo, this operation should still succeed
  // because T1 got a snapshot before T2
  db_catalog = catalog_->GetDatabaseCatalog(common::ManagedPointer(txn1), db_oid);
  auto table_ptr = db_catalog->GetTable(common::ManagedPointer(txn1), table_oid);
  const auto &schema = db_catalog->GetSchema(common::ManagedPointer(txn1), table_oid);
  EXPECT_EQ(1, schema.GetColumns().size());
  EXPECT_EQ(type::TypeId::INTEGER, schema.GetColumn(0).Type());
  auto initializer = table_ptr->InitializerForProjectedRow({schema.GetColumn(0).Oid()});
  auto *redo_record = txn1->StageWrite(db_oid, table_oid, initializer);
  *reinterpret_cast<int32_t *>(redo_record->Delta()->AccessForceNotNull(0)) = 0;
  table_ptr->Insert(common::ManagedPointer(txn1), redo_record);
  txn_manager_->Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

  ShutdownAndRestartSystem();

  // Instantiate recovery manager, and recover the catalog
  SingleRecovery();

  // Assert the database we created does not exists
  auto txn = recovery_txn_manager_->BeginTransaction();
  EXPECT_EQ(db_oid, recovery_catalog_->GetDatabaseOid(common::ManagedPointer(txn), database_name));
  EXPECT_TRUE(recovery_catalog_->GetDatabaseCatalog(common::ManagedPointer(txn), db_oid));

  // Assert the table we deleted doesn't exist
  EXPECT_EQ(catalog::INVALID_TABLE_OID,
            db_catalog->GetTableOid(common::ManagedPointer(txn), namespace_oid, table_name));
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
  auto *tested =
      new LargeSqlTableTestObject(config, txn_manager_.Get(), catalog_.Get(), block_store_.Get(), &generator_);

  // Run workload
  tested->SimulateOltp(100, 4);

  ShutdownAndRestartSystem();

  // Override the recovery DBMain to now log out
  recovery_db_main_ = noisepage::DBMain::Builder()
                          .SetWalFilePath(secondary_log_file)
                          .SetUseLogging(true)
                          .SetUseGC(true)
                          .SetUseGCThread(true)
                          .SetUseCatalog(true)
                          .SetCreateDefaultDatabase(false)
                          .Build();
  recovery_txn_manager_ = recovery_db_main_->GetTransactionLayer()->GetTransactionManager();
  recovery_deferred_action_manager_ = recovery_db_main_->GetTransactionLayer()->GetDeferredActionManager();
  recovery_block_store_ = recovery_db_main_->GetStorageLayer()->GetBlockStore();
  recovery_catalog_ = recovery_db_main_->GetCatalogLayer()->GetCatalog();
  recovery_thread_registry_ = recovery_db_main_->GetThreadRegistry();

  //--------------------------------
  // Do recovery for the first time
  //--------------------------------

  // Instantiate recovery manager, and recover the tables.
  DiskLogProvider log_provider(RECOVERY_TEST_LOG_FILE_NAME);
  RecoveryManager recovery_manager{common::ManagedPointer<AbstractLogProvider>(&log_provider),
                                   recovery_catalog_,
                                   recovery_txn_manager_,
                                   recovery_deferred_action_manager_,
                                   DISABLED,
                                   recovery_thread_registry_,
                                   recovery_block_store_};
  recovery_manager.StartRecovery();
  recovery_manager.WaitForRecoveryToFinish();

  // Check we recovered all the original tables
  for (auto &database : tested->GetTables()) {
    auto database_oid = database.first;
    for (auto &table_oid : database.second) {
      // Get original sql table
      auto original_txn = txn_manager_->BeginTransaction();
      auto original_sql_table = catalog_->GetDatabaseCatalog(common::ManagedPointer(original_txn), database_oid)
                                    ->GetTable(common::ManagedPointer(original_txn), table_oid);

      // Get Recovered table
      auto *recovery_txn = recovery_txn_manager_->BeginTransaction();
      auto db_catalog = recovery_catalog_->GetDatabaseCatalog(common::ManagedPointer(recovery_txn), database_oid);
      EXPECT_TRUE(db_catalog != nullptr);
      auto recovered_sql_table = db_catalog->GetTable(common::ManagedPointer(recovery_txn), table_oid);
      EXPECT_TRUE(recovered_sql_table != nullptr);

      EXPECT_TRUE(StorageTestUtil::SqlTableEqualDeep(
          GetBlockLayout(original_sql_table), original_sql_table, recovered_sql_table,
          tested->GetTupleSlotsForTable(database_oid, table_oid), recovery_manager.tuple_slot_map_, txn_manager_.Get(),
          recovery_txn_manager_.Get()));
      txn_manager_->Commit(original_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
      recovery_txn_manager_->Commit(recovery_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
    }
  }

  // Contrary to other tests, we clean up the recovery catalog and gc thread here because the secondary_log_manager is a
  // local object. Setting the appropriate variables to nullptr will allow the TearDown code to run normally.
  recovery_db_main_.reset();

  log_manager_->PersistAndStop();
  //-----------------------------------------
  // Now recover based off the last recovery
  //-----------------------------------------
  log_manager_->Start();

  // Create a new DBMain with logging disabled
  auto secondary_recovery_db_main = noisepage::DBMain::Builder()
                                        .SetUseThreadRegistry(true)
                                        .SetUseGC(true)
                                        .SetUseGCThread(true)
                                        .SetUseCatalog(true)
                                        .SetCreateDefaultDatabase(false)
                                        .Build();

  auto secondary_recovery_txn_manager = secondary_recovery_db_main->GetTransactionLayer()->GetTransactionManager();
  auto secondary_recovery_deferred_action_manager =
      secondary_recovery_db_main->GetTransactionLayer()->GetDeferredActionManager();
  auto secondary_recovery_block_store = secondary_recovery_db_main->GetStorageLayer()->GetBlockStore();
  auto secondary_recovery_catalog = secondary_recovery_db_main->GetCatalogLayer()->GetCatalog();
  auto secondary_recovery_thread_registry = secondary_recovery_db_main->GetThreadRegistry();

  // Instantiate a new recovery manager, and recover the tables.
  DiskLogProvider secondary_log_provider(secondary_log_file);
  RecoveryManager secondary_recovery_manager(common::ManagedPointer<AbstractLogProvider>(&secondary_log_provider),
                                             secondary_recovery_catalog, secondary_recovery_txn_manager,
                                             secondary_recovery_deferred_action_manager, DISABLED,
                                             secondary_recovery_thread_registry, secondary_recovery_block_store);
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
      auto original_sql_table = catalog_->GetDatabaseCatalog(common::ManagedPointer(original_txn), database_oid)
                                    ->GetTable(common::ManagedPointer(original_txn), table_oid);

      // Get Recovered table
      auto *recovery_txn = secondary_recovery_txn_manager->BeginTransaction();
      auto db_catalog =
          secondary_recovery_catalog->GetDatabaseCatalog(common::ManagedPointer(recovery_txn), database_oid);
      EXPECT_TRUE(db_catalog != nullptr);
      auto recovered_sql_table = db_catalog->GetTable(common::ManagedPointer(recovery_txn), table_oid);
      EXPECT_TRUE(recovered_sql_table != nullptr);

      EXPECT_TRUE(StorageTestUtil::SqlTableEqualDeep(
          GetBlockLayout(original_sql_table), original_sql_table, recovered_sql_table,
          tested->GetTupleSlotsForTable(database_oid, table_oid), new_tuple_slot_map, txn_manager_.Get(),
          secondary_recovery_txn_manager.Get()));
      txn_manager_->Commit(original_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
      secondary_recovery_txn_manager->Commit(recovery_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
    }
  }

  // the table can't be freed until after all GC on it is guaranteed to be done. The easy way to do that is to use a
  // DeferredAction
  db_main_->GetTransactionLayer()->GetDeferredActionManager()->RegisterDeferredAction([=]() { delete tested; });

  secondary_recovery_db_main->GetTransactionLayer()->GetDeferredActionManager()->RegisterDeferredAction(
      [=]() { unlink(secondary_log_file.c_str()); });
}

}  // namespace noisepage::storage
