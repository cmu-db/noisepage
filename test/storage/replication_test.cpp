#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/postgres/pg_namespace.h"
#include "gtest/gtest.h"
#include "main/db_main.h"
#include "network/itp/itp_protocol_interpreter.h"
#include "storage/garbage_collector_thread.h"
#include "storage/index/index_builder.h"
#include "storage/replication/replication_manager.h"
#include "storage/recovery/replication_log_provider.h"
#include "storage/sql_table.h"
#include "storage/write_ahead_log/log_manager.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "test_util/catalog_test_util.h"
#include "test_util/sql_table_test_util.h"
#include "test_util/storage_test_util.h"
#include "test_util/test_harness.h"

// Make sure that if you create additional files, you call unlink on them after the test finishes. Otherwise, repeated
// executions will read old test's data, and the cause of the errors will be hard to identify. Trust me it will drive
// you nuts...
#define LOG_FILE_NAME "./test.log"

namespace terrier::storage {
  class ReplicationTests : public TerrierTest {
  protected:
    std::default_random_engine generator_;
    storage::RecordBufferSegmentPool buffer_pool_{2000, 100};
    storage::BlockStore block_store_{100, 100};

    // Settings for replication
    const std::chrono::seconds replication_timeout_{10};

    // Original Components
    std::unique_ptr<DBMain> master_db_main_;
    common::ManagedPointer<transaction::TransactionManager> master_txn_manager_;
    common::ManagedPointer<storage::LogManager> master_log_manager_;
    common::ManagedPointer<storage::BlockStore> master_block_store_;
    common::ManagedPointer<catalog::Catalog> master_catalog_;
    common::ManagedPointer<storage::ReplicationManager> master_replication_manager_;

    // Recovery Components
    std::unique_ptr<DBMain> replica_db_main_;
    common::ManagedPointer<transaction::TransactionManager> replica_txn_manager_;
    common::ManagedPointer<transaction::DeferredActionManager> replica_deferred_action_manager_;
    common::ManagedPointer<storage::BlockStore> replica_block_store_;
    common::ManagedPointer<catalog::Catalog> replica_catalog_;
    common::ManagedPointer<common::DedicatedThreadRegistry> replica_thread_registry_;
    common::ManagedPointer<storage::ReplicationManager> replica_replication_manager_;
    common::ManagedPointer<storage::ReplicationLogProvider> replica_log_provider_;

    void SetUp() override {
      // Unlink log file incase one exists from previous test iteration
      unlink(LOG_FILE_NAME);

      master_db_main_ = terrier::DBMain::Builder()
          .SetWalFilePath(LOG_FILE_NAME)
          .SetUseLogging(true)
          .SetUseGC(true)
          .SetUseGCThread(true)
          .SetUseCatalog(true)
          .SetReplicationDestination("127.0.0.1", 9022)
          .Build();
      master_txn_manager_ = master_db_main_->GetTransactionLayer()->GetTransactionManager();
      master_log_manager_ = master_db_main_->GetLogManager();
      master_block_store_ = master_db_main_->GetStorageLayer()->GetBlockStore();
      master_catalog_ = master_db_main_->GetCatalogLayer()->GetCatalog();
      master_replication_manager_ = master_db_main_->GetReplicationManager();

      replica_db_main_ = terrier::DBMain::Builder()
          .SetUseThreadRegistry(true)
          .SetUseGC(true)
          .SetUseGCThread(true)
          .SetUseCatalog(true)
          .SetCreateDefaultDatabase(false)
          .SetReplicationDestination("127.0.0.1", 9023)
          .Build();
      replica_txn_manager_ = replica_db_main_->GetTransactionLayer()->GetTransactionManager();
      replica_deferred_action_manager_ = replica_db_main_->GetTransactionLayer()->GetDeferredActionManager();
      replica_block_store_ = replica_db_main_->GetStorageLayer()->GetBlockStore();
      replica_catalog_ = replica_db_main_->GetCatalogLayer()->GetCatalog();
      replica_thread_registry_ = replica_db_main_->GetThreadRegistry();
      replica_replication_manager_ = replica_db_main_->GetReplicationManager();
      replica_log_provider_ = common::ManagedPointer(new ReplicationLogProvider(replication_timeout_));
      replica_recovery_manager_ =  new RecoveryManager(common::ManagedPointer<storage::AbstractLogProvider>(replica_log_provider_),
                                                       common::ManagedPointer(replica_catalog_), replica_txn_manager_,
                                                       replica_deferred_action_manager_,
                                                       common::ManagedPointer(replica_thread_registry_), &block_store_);
    }

    catalog::db_oid_t CreateDatabase(transaction::TransactionContext *txn,
                                     common::ManagedPointer<catalog::Catalog> catalog, const std::string &database_name) {
      auto db_oid = catalog->CreateDatabase(common::ManagedPointer(txn), database_name, true /* bootstrap */);
      EXPECT_TRUE(db_oid != catalog::INVALID_DATABASE_OID);
      return db_oid;
    }

    void TearDown() override {
      // Delete log file
      unlink(LOG_FILE_NAME);
    }

  };

  TEST_F(ReplicationTests, InitializationTest) {
    STORAGE_LOG_ERROR("start");
    std::string database_name = "testdb";
    // Create a database and commit, we should see this one after replication
    auto *txn = master_txn_manager_->BeginTransaction();
    CreateDatabase(txn, master_catalog_, database_name);
    master_txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    auto replication_delay_estimate_ = std::chrono::seconds(2);
    std::this_thread::sleep_for(replication_delay_estimate_);

    master_replication_manager_->SendMessage();

    STORAGE_LOG_ERROR("end");
  }

}  // namespace terrier::storage
