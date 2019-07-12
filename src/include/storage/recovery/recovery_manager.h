#pragma once

#include <catalog/catalog.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/dedicated_thread_owner.h"
#include "storage/recovery/abstract_log_provider.h"
#include "storage/sql_table.h"
#include "transaction/transaction_manager.h"

namespace terrier::storage {

/**
 * Recovery Manager
 * TODO(Gus): Add more documentation when API is finalized
 */
class RecoveryManager : public common::DedicatedThreadOwner {
  /**
   * Task in charge of initializing recovery. This way recovery can be non-blocking in a background thread.
   */
  class RecoveryTask : public common::DedicatedThreadTask {
   public:
    /**
     * @param recovery_manager pointer to recovery manager who initialized task
     */
    explicit RecoveryTask(RecoveryManager *recovery_manager) : recovery_manager_(recovery_manager) {}

    /**
     * Runs the recovery task. Our task only calls Recover on the log manager.
     */
    void RunTask() override { recovery_manager_->Recover(); }

    /**
     * Terminate does nothing, the task will terminate when RunTask() returns. In the future if we need to support
     * interrupting recovery, this can be handled here.
     */
    void Terminate() override {}

   private:
    RecoveryManager *recovery_manager_;
  };

 public:
  /**
   * @param log_provider arbitrary provider to receive logs from
   * @param catalog system catalog to interface with sql tables
   * @param txn_manager txn manager to use for re-executing recovered transactions
   * @param thread_registry thread registry to register tasks
   */
  explicit RecoveryManager(AbstractLogProvider *log_provider, common::ManagedPointer<catalog::Catalog> *catalog,
                           transaction::TransactionManager *txn_manager,
                           common::ManagedPointer<terrier::common::DedicatedThreadRegistry> thread_registry)
      : DedicatedThreadOwner(thread_registry),
        log_provider_(log_provider),
        catalog_(catalog),
        txn_manager_(txn_manager),
        recovered_txns_(0) {}

  /**
   * Starts a background recovery task. Recovery will fully recover until the log provider stops providing logs.
   */
  void StartRecovery() {
    TERRIER_ASSERT(recovery_task_ == nullptr, "Recovery already started");
    recovery_task_ =
        thread_registry_->RegisterDedicatedThread<RecoveryTask>(this /* dedicated thread owner */, this /* task arg */);
  }

  /**
   * Stops the background recovery task. This will block until recovery finishes, if it has not already.
   */
  void FinishRecovery() {
    TERRIER_ASSERT(recovery_task_ != nullptr, "Recovery must already have been started");
    bool result UNUSED_ATTRIBUTE =
        thread_registry_->StopTask(this, recovery_task_.CastManagedPointerTo<common::DedicatedThreadTask>());
    TERRIER_ASSERT(result, "Task termination should always succeed");
  }

  /**
   * @return number of committed txns recovered so far
   */
  uint32_t GetRecoveredTxnCount() const { return recovered_txns_; }

 private:
  FRIEND_TEST(RecoveryTests, SingleTableTest);
  FRIEND_TEST(RecoveryTests, HighAbortRateTest);
  FRIEND_TEST(RecoveryTests, MultiDatabaseTest);

  // Log provider for reading in logs
  AbstractLogProvider *log_provider_;

  // Catalog to fetch table pointers
  common::ManagedPointer<catalog::Catalog> *catalog_;

  // Transaction manager to create transactions for recovery
  transaction::TransactionManager *txn_manager_;

  // Used during recovery from log. Maps old tuple slot to new tuple slot
  // TODO(Gus): This map may get huge, benchmark whether this becomes a problem and if we need a more sophisticated data
  // structure
  std::unordered_map<TupleSlot, TupleSlot> tuple_slot_map_;

  // Used during recovery from log. Maps a the txn id from the persisted txn to its changes we have buffered. We buffer
  // changes until commit time. This ensures serializability, and allows us to skip changes from aborted txns.
  std::unordered_map<transaction::timestamp_t, std::vector<std::pair<LogRecord *, std::vector<byte *>>>>
      buffered_changes_map_;

  // Background recovery task
  common::ManagedPointer<RecoveryTask> recovery_task_ = nullptr;

  // Number of recovered txns. Used for benchmarking
  uint32_t recovered_txns_;

  /**
   * Recovers the databases using the provided log provider
   * @return number of committed transactions replayed
   */
  void Recover() { recovered_txns_ += RecoverFromLogs(); }

  /**
   * Recovers the databases from the logs.
   * @note this is a separate method so in the future, we can also have a RecoverFromCheckpoint method
   * @return number of committed txns replayed
   */
  uint32_t RecoverFromLogs();

  /**
   * @brief Replays a transaction corresponding to the given log record log record.
   * @param log_record abort or commit record for transaction to replay
   */
  void ReplayTransaction(LogRecord *log_record);

  /**
   * Wrapper over GetDatabaseCatalog method that asserts the database exists
   * @param txn txn for catalog lookup
   * @param database oid for database we want
   * @return pointer to database catalog
   */
  common::ManagedPointer<catalog::DatabaseCatalog> GetDatabaseCatalog(transaction::TransactionContext *txn,
                                                                      catalog::db_oid_t db_oid) {
    auto db_catalog_ptr = catalog_->GetDatabaseCatalog(txn, db_oid);
    TERRIER_ASSERT(db_catalog_ptr != nullptr, "No catalog for given database oid");
    return db_catalog_ptr;
  }

  /**
   * @param txn transaction to use for catalog lookup
   * @param db_oid database oid for requested table
   * @param table_oid table oid for requested table
   * @return pointer to requested Sql table
   */
  common::ManagedPointer<storage::SqlTable> GetSqlTable(transaction::TransactionContext *txn, catalog::db_oid_t db_oid,
                                                         catalog::table_oid_t table_oid) {
    auto db_catalog_ptr = GetDatabaseCatalog(txn, db_oid);
    auto table_ptr = db_catalog_ptr->GetTable(txn, table_oid);
    TERRIER_ASSERT(table_ptr != nullptr, "Table in the catalog for the given oid");
    return table_ptr;
  }

  /**
   * Inserts or deletes a tuple slot from all indexes on a table.
   * @warning For an insert, must be called after the tuple slot is inserted into the table, for a delete, it must be
   * called before it is deleted from the table
   * @param txn transaction to delete with
   * @param db_oid database oid for table
   * @param table_oid indexed table
   * @param tuple tuple slot to delete
   * @param insert true if we should insert into the index, false if we should delete
   */
  void UpdateIndexesOnTable(transaction::TransactionContext *txn, const catalog::db_oid_t db_oid,
                            const catalog::table_oid_t table_oid, const TupleSlot &tuple, bool insert);
};
}  // namespace terrier::storage
