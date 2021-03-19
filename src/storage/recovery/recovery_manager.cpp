#include "storage/recovery/recovery_manager.h"

#include <algorithm>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/postgres/pg_attribute.h"
#include "catalog/postgres/pg_class.h"
#include "catalog/postgres/pg_constraint.h"
#include "catalog/postgres/pg_database.h"
#include "catalog/postgres/pg_index.h"
#include "catalog/postgres/pg_language.h"
#include "catalog/postgres/pg_namespace.h"
#include "catalog/postgres/pg_proc.h"
#include "catalog/postgres/pg_type.h"
#include "common/dedicated_thread_registry.h"
#include "common/json.h"
#include "replication/replication_manager.h"
#include "storage/index/index.h"
#include "storage/index/index_builder.h"
#include "storage/index/index_metadata.h"
#include "storage/recovery/replication_log_provider.h"
#include "storage/write_ahead_log/log_io.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_manager.h"

namespace noisepage::storage {

void RecoveryManager::StartRecovery() {
  NOISEPAGE_ASSERT(recovery_task_ == nullptr, "Recovery already started");
  recovery_task_loop_again_ = true;  // RecoveryTask will loop by default to enable replication use cases.
  recovery_task_ =
      thread_registry_->RegisterDedicatedThread<RecoveryTask>(this /* dedicated thread owner */, this /* task arg */);
}

void RecoveryManager::WaitForRecoveryToFinish() {
  recovery_task_loop_again_ = false;  // Stop looping RecoveryTask.
  NOISEPAGE_ASSERT(recovery_task_ != nullptr, "Recovery must already have been started");
  if (!thread_registry_->StopTask(this, recovery_task_.CastManagedPointerTo<common::DedicatedThreadTask>())) {
    throw std::runtime_error("Recovery task termination failed");
  }
  recovery_task_ = nullptr;
}

void RecoveryManager::RecoverFromLogs(const common::ManagedPointer<AbstractLogProvider> log_provider) {
  // Replay logs until the log provider no longer gives us logs
  while (true) {
    if (replication_manager_ != DISABLED &&
        log_provider->GetType() == AbstractLogProvider::LogProviderType::REPLICATION) {
      auto rep_log_provider = log_provider.CastManagedPointerTo<ReplicationLogProvider>();
      if (!rep_log_provider->NonBlockingHasMoreRecords()) break;
    }

    auto pair = log_provider->GetNextRecord();
    auto *log_record = pair.first;

    // If we have exhausted all the logs, break from the loop
    if (log_record == nullptr) break;

    switch (log_record->RecordType()) {
      case (LogRecordType::ABORT): {
        NOISEPAGE_ASSERT(pair.second.empty(), "Abort records should not have any varlen pointers");
        DeferRecordDeletes(log_record->TxnBegin(), true);
        buffered_changes_map_.erase(log_record->TxnBegin());
        deferred_action_manager_->RegisterDeferredAction([=] { delete[] reinterpret_cast<byte *>(log_record); });
        break;
      }

      case (LogRecordType::COMMIT): {
        NOISEPAGE_ASSERT(pair.second.empty(), "Commit records should not have any varlen pointers");
        auto *commit_record = log_record->GetUnderlyingRecordBodyAs<CommitRecord>();

        // We defer all transactions initially
        deferred_txns_.insert(log_record->TxnBegin());

        // Process any deferred transactions that are safe to execute
        recovered_txns_ += ProcessDeferredTransactions(commit_record->OldestActiveTxn());

        // Clean up the log record
        deferred_action_manager_->RegisterDeferredAction([=] { delete[] reinterpret_cast<byte *>(log_record); });
        break;
      }

      default:
        NOISEPAGE_ASSERT(
            log_record->RecordType() == LogRecordType::REDO || log_record->RecordType() == LogRecordType::DELETE,
            "We should only buffer changes for redo or delete records");
        buffered_changes_map_[log_record->TxnBegin()].push_back(pair);
    }
  }
  // Process all deferred txns
  ProcessDeferredTransactions(transaction::INVALID_TXN_TIMESTAMP);
  NOISEPAGE_ASSERT(deferred_txns_.empty(),
                   "We should have no unprocessed deferred transactions at the end of recovery");

  // If we have unprocessed buffered changes, then these transactions were in-process at the time of system shutdown.
  // They are unrecoverable, so we need to clean up the memory of their records.
  if (!buffered_changes_map_.empty()) {
    for (const auto &txn : buffered_changes_map_) {
      DeferRecordDeletes(txn.first, true);
    }
    buffered_changes_map_.clear();
  }

  if (replication_manager_ != DISABLED &&
      log_provider->GetType() == AbstractLogProvider::LogProviderType::REPLICATION) {
    auto rep_log_provider = log_provider.CastManagedPointerTo<ReplicationLogProvider>();
    rep_log_provider->LatchPrimaryAckables();
    std::vector<uint64_t> &ackables = rep_log_provider->GetPrimaryAckables();
    std::vector<uint64_t> ackables_copy = ackables;
    ackables.clear();
    rep_log_provider->UnlatchPrimaryAckables();
    for (const uint64_t primary_cb_id : ackables_copy) {
      replication_manager_->ReplicaAck("primary", primary_cb_id, false);
    }
  }
}

void RecoveryManager::ProcessCommittedTransaction(noisepage::transaction::timestamp_t txn_id) {
  // Begin a txn to replay changes with.
  auto *txn = txn_manager_->BeginTransaction();

  // Apply all buffered changes. They should all succeed. After applying we can safely delete the record
  for (uint32_t idx = 0; idx < buffered_changes_map_[txn_id].size(); idx++) {
    auto *buffered_record = buffered_changes_map_[txn_id][idx].first;
    NOISEPAGE_ASSERT(
        buffered_record->RecordType() == LogRecordType::REDO || buffered_record->RecordType() == LogRecordType::DELETE,
        "Buffered record must be a redo or delete.");

    if (IsSpecialCaseCatalogRecord(buffered_record)) {
      idx += ProcessSpecialCaseCatalogRecord(txn, &buffered_changes_map_[txn_id], idx);
    } else if (buffered_record->RecordType() == LogRecordType::REDO) {
      ReplayRedoRecord(txn, buffered_record);
    } else {
      ReplayDeleteRecord(txn, buffered_record);
    }
  }

  // Defer deletes of the log records
  DeferRecordDeletes(txn_id, false);
  buffered_changes_map_.erase(txn_id);

  // Commit the txn
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

void RecoveryManager::DeferRecordDeletes(noisepage::transaction::timestamp_t txn_id, bool delete_varlens) {
  // Capture the changes by value except for changes which we can move
  deferred_action_manager_->RegisterDeferredAction([=, buffered_changes{std::move(buffered_changes_map_[txn_id])}]() {
    for (auto &buffered_pair : buffered_changes) {
      delete[] reinterpret_cast<byte *>(buffered_pair.first);
      if (delete_varlens) {
        for (auto *varlen_entry : buffered_pair.second) {
          delete[] varlen_entry;
        }
      }
    }
  });
}

uint32_t RecoveryManager::ProcessDeferredTransactions(noisepage::transaction::timestamp_t upper_bound_ts) {
  auto txns_processed = 0;
  // If the upper bound is INVALID_TXN_TIMESTAMP, then we should process all deferred txns. We can accomplish this by
  // setting the upper bound to INT_MAX
  upper_bound_ts =
      (upper_bound_ts == transaction::INVALID_TXN_TIMESTAMP) ? transaction::timestamp_t(INT64_MAX) : upper_bound_ts;
  auto upper_bound_it = deferred_txns_.upper_bound(upper_bound_ts);

  for (auto it = deferred_txns_.begin(); it != upper_bound_it; it++) {
    ProcessCommittedTransaction(*it);
    txns_processed++;
  }

  // If we actually processed some txns, remove them from the set
  if (txns_processed > 0) deferred_txns_.erase(deferred_txns_.begin(), upper_bound_it);

  return txns_processed;
}

void RecoveryManager::ReplayRedoRecord(transaction::TransactionContext *txn, LogRecord *record) {
  auto *redo_record = record->GetUnderlyingRecordBodyAs<RedoRecord>();
  auto sql_table_ptr = GetSqlTable(txn, redo_record->GetDatabaseOid(), redo_record->GetTableOid());
  if (IsInsertRecord(redo_record)) {
    // Save the old tuple slot, and reset the tuple slot in the record
    auto old_tuple_slot = redo_record->GetTupleSlot();
    redo_record->SetTupleSlot(TupleSlot(nullptr, 0));
    // Stage the write. This way the recovery operation is logged if logging is enabled.
    auto staged_record = txn->StageRecoveryWrite(record);
    NOISEPAGE_ASSERT(redo_record->Delta()->Size() == staged_record->Delta()->Size(),
                     "Redo record must be the same size after staging in recovery");
    NOISEPAGE_ASSERT(memcmp(redo_record->Delta(), staged_record->Delta(), redo_record->Delta()->Size()) == 0,
                     "ProjectedRow of original and staged records must be identical");
    // Insert will always succeed
    auto new_tuple_slot = sql_table_ptr->Insert(common::ManagedPointer(txn), staged_record);
    UpdateIndexesOnTable(txn, staged_record->GetDatabaseOid(), staged_record->GetTableOid(), sql_table_ptr,
                         new_tuple_slot, staged_record->Delta(), true /* insert */);
    NOISEPAGE_ASSERT(staged_record->GetTupleSlot() == new_tuple_slot,
                     "Insert should update redo record with new tuple slot");
    // Create a mapping of the old to new tuple. The new tuple slot should be used for future updates and deletes.
    tuple_slot_map_[old_tuple_slot] = new_tuple_slot;
  } else {
    auto new_tuple_slot = tuple_slot_map_[redo_record->GetTupleSlot()];
    redo_record->SetTupleSlot(new_tuple_slot);
    // Stage the write. This way the recovery operation is logged if logging is enabled
    auto staged_record = txn->StageRecoveryWrite(record);
    NOISEPAGE_ASSERT(staged_record->GetTupleSlot() == new_tuple_slot, "Staged record must have the mapped tuple slot");
    bool result UNUSED_ATTRIBUTE = sql_table_ptr->Update(common::ManagedPointer(txn), staged_record);
    NOISEPAGE_ASSERT(result, "Buffered changes should always succeed during commit");
  }
}

void RecoveryManager::ReplayDeleteRecord(transaction::TransactionContext *txn, LogRecord *record) {
  auto *delete_record = record->GetUnderlyingRecordBodyAs<DeleteRecord>();
  // Get tuple slot
  auto new_tuple_slot = GetTupleSlotMapping(delete_record->GetTupleSlot());
  auto db_catalog_ptr = GetDatabaseCatalog(txn, delete_record->GetDatabaseOid());
  auto sql_table_ptr = db_catalog_ptr->GetTable(common::ManagedPointer(txn), delete_record->GetTableOid());
  const auto &schema = GetTableSchema(txn, db_catalog_ptr, delete_record->GetTableOid());

  // Stage the delete. This way the recovery operation is logged if logging is enabled
  txn->StageDelete(delete_record->GetDatabaseOid(), delete_record->GetTableOid(), new_tuple_slot);

  // Fetch all the values so we can construct index keys after deleting from the sql table
  std::vector<catalog::col_oid_t> all_table_oids;
  for (const auto &col : schema.GetColumns()) {
    all_table_oids.push_back(col.Oid());
  }
  auto initializer = sql_table_ptr->InitializerForProjectedRow(all_table_oids);
  auto *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
  auto pr = initializer.InitializeRow(buffer);
  sql_table_ptr->Select(common::ManagedPointer(txn), new_tuple_slot, pr);

  // Delete from the table
  bool result UNUSED_ATTRIBUTE = sql_table_ptr->Delete(common::ManagedPointer(txn), new_tuple_slot);
  NOISEPAGE_ASSERT(result, "Buffered changes should always succeed during commit");

  // Delete from the indexes
  UpdateIndexesOnTable(txn, delete_record->GetDatabaseOid(), delete_record->GetTableOid(), sql_table_ptr,
                       new_tuple_slot, pr, false /* delete */);
  // We can delete the TupleSlot from the map
  tuple_slot_map_.erase(delete_record->GetTupleSlot());
  delete[] buffer;
}

void RecoveryManager::UpdateIndexesOnTable(transaction::TransactionContext *txn, catalog::db_oid_t db_oid,
                                           catalog::table_oid_t table_oid,
                                           common::ManagedPointer<storage::SqlTable> table_ptr,
                                           const TupleSlot &tuple_slot, ProjectedRow *table_pr, const bool insert) {
  auto db_catalog_ptr = GetDatabaseCatalog(txn, db_oid);

  // Stores index objects and schemas
  std::vector<std::pair<common::ManagedPointer<index::Index>, const catalog::IndexSchema &>> index_objects;

  // We don't bootstrap the database catalog during recovery, so this means that indexes on catalog tables may not yet
  // be entries in pg_index. Thus, we hardcode these to update
  switch (table_oid.UnderlyingValue()) {
    case (catalog::postgres::PgDatabase::DATABASE_TABLE_OID.UnderlyingValue()): {
      index_objects.emplace_back(catalog_->databases_name_index_,
                                 catalog_->databases_name_index_->metadata_.GetSchema());
      index_objects.emplace_back(catalog_->databases_oid_index_, catalog_->databases_oid_index_->metadata_.GetSchema());
      break;
    }

    case (catalog::postgres::PgNamespace::NAMESPACE_TABLE_OID.UnderlyingValue()): {
      index_objects.emplace_back(db_catalog_ptr->pg_core_.namespaces_oid_index_,
                                 db_catalog_ptr->pg_core_.namespaces_oid_index_->metadata_.GetSchema());
      index_objects.emplace_back(db_catalog_ptr->pg_core_.namespaces_name_index_,
                                 db_catalog_ptr->pg_core_.namespaces_name_index_->metadata_.GetSchema());
      break;
    }

    case (catalog::postgres::PgClass::CLASS_TABLE_OID.UnderlyingValue()): {
      index_objects.emplace_back(db_catalog_ptr->pg_core_.classes_oid_index_,
                                 db_catalog_ptr->pg_core_.classes_oid_index_->metadata_.GetSchema());
      index_objects.emplace_back(db_catalog_ptr->pg_core_.classes_name_index_,
                                 db_catalog_ptr->pg_core_.classes_name_index_->metadata_.GetSchema());
      index_objects.emplace_back(db_catalog_ptr->pg_core_.classes_namespace_index_,
                                 db_catalog_ptr->pg_core_.classes_namespace_index_->metadata_.GetSchema());
      break;
    }

    case (catalog::postgres::PgAttribute::COLUMN_TABLE_OID.UnderlyingValue()): {
      index_objects.emplace_back(db_catalog_ptr->pg_core_.columns_oid_index_,
                                 db_catalog_ptr->pg_core_.columns_oid_index_->metadata_.GetSchema());
      index_objects.emplace_back(db_catalog_ptr->pg_core_.columns_name_index_,
                                 db_catalog_ptr->pg_core_.columns_name_index_->metadata_.GetSchema());
      break;
    }

    case (catalog::postgres::PgConstraint::CONSTRAINT_TABLE_OID.UnderlyingValue()): {
      index_objects.emplace_back(db_catalog_ptr->pg_constraint_.constraints_oid_index_,
                                 db_catalog_ptr->pg_constraint_.constraints_oid_index_->metadata_.GetSchema());
      index_objects.emplace_back(db_catalog_ptr->pg_constraint_.constraints_name_index_,
                                 db_catalog_ptr->pg_constraint_.constraints_name_index_->metadata_.GetSchema());
      index_objects.emplace_back(db_catalog_ptr->pg_constraint_.constraints_namespace_index_,
                                 db_catalog_ptr->pg_constraint_.constraints_namespace_index_->metadata_.GetSchema());
      index_objects.emplace_back(db_catalog_ptr->pg_constraint_.constraints_index_index_,
                                 db_catalog_ptr->pg_constraint_.constraints_index_index_->metadata_.GetSchema());
      index_objects.emplace_back(db_catalog_ptr->pg_constraint_.constraints_foreigntable_index_,
                                 db_catalog_ptr->pg_constraint_.constraints_foreigntable_index_->metadata_.GetSchema());
      break;
    }

    case (catalog::postgres::PgIndex::INDEX_TABLE_OID.UnderlyingValue()): {
      index_objects.emplace_back(db_catalog_ptr->pg_core_.indexes_oid_index_,
                                 db_catalog_ptr->pg_core_.indexes_oid_index_->metadata_.GetSchema());
      index_objects.emplace_back(db_catalog_ptr->pg_core_.indexes_table_index_,
                                 db_catalog_ptr->pg_core_.indexes_table_index_->metadata_.GetSchema());
      break;
    }

    case (catalog::postgres::PgType::TYPE_TABLE_OID.UnderlyingValue()): {
      index_objects.emplace_back(db_catalog_ptr->pg_type_.types_oid_index_,
                                 db_catalog_ptr->pg_type_.types_oid_index_->metadata_.GetSchema());
      index_objects.emplace_back(db_catalog_ptr->pg_type_.types_name_index_,
                                 db_catalog_ptr->pg_type_.types_name_index_->metadata_.GetSchema());
      index_objects.emplace_back(db_catalog_ptr->pg_type_.types_namespace_index_,
                                 db_catalog_ptr->pg_type_.types_namespace_index_->metadata_.GetSchema());
      break;
    }

    case (catalog::postgres::PgLanguage::LANGUAGE_TABLE_OID.UnderlyingValue()): {
      index_objects.emplace_back(db_catalog_ptr->pg_language_.languages_oid_index_,
                                 db_catalog_ptr->pg_language_.languages_oid_index_->metadata_.GetSchema());
      index_objects.emplace_back(db_catalog_ptr->pg_language_.languages_name_index_,
                                 db_catalog_ptr->pg_language_.languages_name_index_->metadata_.GetSchema());
      break;
    }
    case (catalog::postgres::PgProc::PRO_TABLE_OID.UnderlyingValue()): {
      index_objects.emplace_back(db_catalog_ptr->pg_proc_.procs_oid_index_,
                                 db_catalog_ptr->pg_proc_.procs_oid_index_->metadata_.GetSchema());
      index_objects.emplace_back(db_catalog_ptr->pg_proc_.procs_name_index_,
                                 db_catalog_ptr->pg_proc_.procs_name_index_->metadata_.GetSchema());
      break;
    }

    default:  // Non-catalog table
      index_objects = db_catalog_ptr->GetIndexes(common::ManagedPointer(txn), table_oid);
  }

  // If there's no indexes on the table, we can return
  if (index_objects.empty()) return;

  // Compute largest PR size we need for index PRs.
  uint32_t max_index_key_pr_size = 0;
  for (const auto &index_obj : index_objects) {
    max_index_key_pr_size =
        std::max(max_index_key_pr_size, index_obj.first->GetProjectedRowInitializer().ProjectedRowSize());
  }
  auto *index_buffer = common::AllocationUtil::AllocateAligned(max_index_key_pr_size);

  // Build a PR map for all columns in the table, as the table pr should have values for every column
  const auto &table_schema = GetTableSchema(txn, db_catalog_ptr, table_oid);
  std::vector<catalog::col_oid_t> all_table_oids;
  for (const auto &col : table_schema.GetColumns()) {
    all_table_oids.push_back(col.Oid());
  }
  auto pr_map = table_ptr->ProjectionMapForOids(all_table_oids);
  NOISEPAGE_ASSERT(pr_map.size() == table_pr->NumColumns(), "Projected row should contain all attributes");

  // TODO(Gus): We are going to assume no indexes on expressions below. Having indexes on expressions would require to
  // evaluate expressions and that's a nightmare
  for (const auto &index_obj : index_objects) {
    auto index = index_obj.first;
    const auto &schema = index_obj.second;
    const auto &indexed_attributes = schema.GetIndexedColOids();

    // Build the index PR
    auto *index_pr = index->GetProjectedRowInitializer().InitializeRow(index_buffer);

    // Copy in each value from the table PR into the index PR
    auto num_index_cols = schema.GetColumns().size();
    NOISEPAGE_ASSERT(num_index_cols == indexed_attributes.size(),
                     "Only support index keys that are a single column oid");
    for (uint32_t col_idx = 0; col_idx < num_index_cols; col_idx++) {
      const auto &col = schema.GetColumn(col_idx);
      auto index_col_oid = col.Oid();
      const catalog::col_oid_t &table_col_oid = indexed_attributes[col_idx];
      if (table_pr->IsNull(pr_map[table_col_oid])) {
        index_pr->SetNull(index->GetKeyOidToOffsetMap().at(index_col_oid));
      } else {
        auto size = AttrSizeBytes(col.AttributeLength());
        std::memcpy(index_pr->AccessForceNotNull(index->GetKeyOidToOffsetMap().at(index_col_oid)),
                    table_pr->AccessWithNullCheck(pr_map[table_col_oid]), size);
      }
    }

    if (insert) {
      bool result UNUSED_ATTRIBUTE = (index->metadata_.GetSchema().Unique())
                                         ? index->InsertUnique(common::ManagedPointer(txn), *index_pr, tuple_slot)
                                         : index->Insert(common::ManagedPointer(txn), *index_pr, tuple_slot);
      NOISEPAGE_ASSERT(result, "Insert into index should always succeed for a committed transaction");
    } else {
      index->Delete(common::ManagedPointer(txn), *index_pr, tuple_slot);
    }
  }

  delete[] index_buffer;
}

uint32_t RecoveryManager::ProcessSpecialCaseCatalogRecord(
    transaction::TransactionContext *txn, std::vector<std::pair<LogRecord *, std::vector<byte *>>> *buffered_changes,
    uint32_t start_idx) {
  auto *curr_record = buffered_changes->at(start_idx).first;
  NOISEPAGE_ASSERT(
      curr_record->RecordType() == LogRecordType::REDO || curr_record->RecordType() == LogRecordType::DELETE,
      "Special case catalog record must be redo or delete");

  // Get oid of table this record modifies
  catalog::table_oid_t table_oid;
  if (curr_record->RecordType() == LogRecordType::REDO) {
    table_oid = curr_record->GetUnderlyingRecordBodyAs<RedoRecord>()->GetTableOid();
  } else {
    table_oid = curr_record->GetUnderlyingRecordBodyAs<DeleteRecord>()->GetTableOid();
  }

  switch (table_oid.UnderlyingValue()) {
    case (catalog::postgres::PgDatabase::DATABASE_TABLE_OID.UnderlyingValue()): {
      return ProcessSpecialCasePGDatabaseRecord(txn, buffered_changes, start_idx);
    }

    case (catalog::postgres::PgClass::CLASS_TABLE_OID.UnderlyingValue()): {
      return ProcessSpecialCasePGClassRecord(txn, buffered_changes, start_idx);
    }

    case (catalog::postgres::PgAttribute::COLUMN_TABLE_OID.UnderlyingValue()): {
      NOISEPAGE_ASSERT(curr_record->RecordType() == LogRecordType::DELETE,
                       "Special case pg_attribute record must be a delete");
      // A delete into pg_attribute means we are deleting a column. There are two cases:
      //  1. Drop column: This requires some additional processing to actually clean up the column
      //  2. Cascading delete from drop table: In this case, we don't process the record because the DeleteTable catalog
      //  function will clean up the columns
      // We currently don't support Case 1
      return 0;  // Case 2, no additional records processed
    }

    case (catalog::postgres::PgIndex::INDEX_TABLE_OID.UnderlyingValue()): {
      NOISEPAGE_ASSERT(curr_record->RecordType() == LogRecordType::DELETE,
                       "Special case pg_index record must be a delete");
      // A delete into pg_index means we are dropping an index. In this case, we dont process the record because the
      // DeleteIndex catalog function will cleanup the entry in pg_index for us.
      return 0;  // No additional logs processed
    }

    case (catalog::postgres::PgProc::PRO_TABLE_OID.UnderlyingValue()): {
      return ProcessSpecialCasePGProcRecord(txn, buffered_changes, start_idx);
    }

    default:
      throw std::logic_error("This table does not require special case logic for record processing");
  }
}

uint32_t RecoveryManager::ProcessSpecialCasePGDatabaseRecord(
    noisepage::transaction::TransactionContext *txn,
    std::vector<std::pair<noisepage::storage::LogRecord *, std::vector<noisepage::byte *>>> *buffered_changes,
    uint32_t start_idx) {
  auto *curr_record = buffered_changes->at(start_idx).first;

  if (curr_record->RecordType() == LogRecordType::REDO) {
    auto *redo_record = curr_record->GetUnderlyingRecordBodyAs<RedoRecord>();
    NOISEPAGE_ASSERT(redo_record->GetTableOid() == catalog::postgres::PgDatabase::DATABASE_TABLE_OID,
                     "This function must be only called with records modifying pg_database");

    // An insert into pg_database is a special case because we need the catalog to actually create the necessary
    // database catalog objects
    NOISEPAGE_ASSERT(IsInsertRecord(redo_record), "Special case on pg_database should only be insert");

    // Step 1: Extract inserted values from the PR in redo record
    const auto pg_database = common::ManagedPointer<storage::SqlTable>(catalog_->databases_);
    auto pr_map = pg_database->ProjectionMapForOids(GetOidsForRedoRecord(pg_database, redo_record));
    NOISEPAGE_ASSERT(pr_map.find(catalog::postgres::PgDatabase::DATOID.oid_) != pr_map.end(),
                     "PR Map must contain database oid");
    NOISEPAGE_ASSERT(pr_map.find(catalog::postgres::PgDatabase::DATNAME.oid_) != pr_map.end(),
                     "PR Map must contain database name");
    catalog::db_oid_t db_oid(*(reinterpret_cast<uint32_t *>(
        redo_record->Delta()->AccessWithNullCheck(pr_map[catalog::postgres::PgDatabase::DATOID.oid_]))));
    VarlenEntry name_varlen = *(reinterpret_cast<VarlenEntry *>(
        redo_record->Delta()->AccessWithNullCheck(pr_map[catalog::postgres::PgDatabase::DATNAME.oid_])));
    std::string name_string(name_varlen.StringView());

    // Step 2: Recreate the database
    auto result UNUSED_ATTRIBUTE = catalog_->CreateDatabase(common::ManagedPointer(txn), name_string, false, db_oid);
    NOISEPAGE_ASSERT(result, "Database recreation should succeed");
    catalog_->UpdateNextOid(db_oid);
    // Manually bootstrap the PRIs
    catalog_->GetDatabaseCatalog(common::ManagedPointer(txn), db_oid)->BootstrapPRIs();

    // Step 3: Update metadata. We need to use the indexes on pg_database to find what tuple slot we just inserted into.
    // We get the new tuple slot using the oid index.
    auto pg_database_oid_index = catalog_->databases_oid_index_;
    auto pr_init = pg_database_oid_index->GetProjectedRowInitializer();
    auto *buffer = common::AllocationUtil::AllocateAligned(pr_init.ProjectedRowSize());
    auto *pr = pr_init.InitializeRow(buffer);
    *(reinterpret_cast<catalog::db_oid_t *>(pr->AccessForceNotNull(0))) = db_oid;
    std::vector<TupleSlot> tuple_slot_result;
    pg_database_oid_index->ScanKey(*txn, *pr, &tuple_slot_result);
    NOISEPAGE_ASSERT(tuple_slot_result.size() == 1, "Index scan should only yield one result");
    tuple_slot_map_[redo_record->GetTupleSlot()] = tuple_slot_result[0];
    delete[] buffer;

    return 0;  // No additional records processed
  }

  NOISEPAGE_ASSERT(curr_record->RecordType() == LogRecordType::DELETE, "Must be delete record at this point");
  auto *delete_record = curr_record->GetUnderlyingRecordBodyAs<DeleteRecord>();

  NOISEPAGE_ASSERT(delete_record->GetTableOid() == catalog::postgres::PgDatabase::DATABASE_TABLE_OID,
                   "Special case for delete should be on pg_class or pg_database");

  // Step 1: Determine the database oid for the database that is being deleted
  const auto pg_database = common::ManagedPointer<storage::SqlTable>(catalog_->databases_);
  auto pr_init = pg_database->InitializerForProjectedRow({catalog::postgres::PgDatabase::DATOID.oid_});
  auto pr_map = pg_database->ProjectionMapForOids({catalog::postgres::PgDatabase::DATOID.oid_});
  auto *buffer = common::AllocationUtil::AllocateAligned(pr_init.ProjectedRowSize());
  auto *pr = pr_init.InitializeRow(buffer);
  pg_database->Select(common::ManagedPointer(txn), GetTupleSlotMapping(delete_record->GetTupleSlot()), pr);
  auto db_oid = *(reinterpret_cast<catalog::db_oid_t *>(
      pr->AccessWithNullCheck(pr_map[catalog::postgres::PgDatabase::DATOID.oid_])));
  delete[] buffer;

  // Step 2: We need to handle the case where we are just renaming a database, in this case we don't wan't to delete
  // the database. A rename appears as a delete followed by an insert with the same OID. See code comment above delete
  // records into pg_class for more detailed breakdown.
  if (start_idx + 1 < buffered_changes->size()) {  // there is one more record
    auto *next_record = buffered_changes->at(start_idx + 1).first;
    if (next_record->RecordType() == LogRecordType::REDO) {  // next record is a redo record
      auto *next_redo_record = next_record->GetUnderlyingRecordBodyAs<RedoRecord>();
      if (next_redo_record->GetDatabaseOid() == delete_record->GetDatabaseOid() &&
          next_redo_record->GetTableOid() == delete_record->GetTableOid() &&
          IsInsertRecord(next_redo_record)) {  // next record is an insert into the same pg_class
        // Step 3: Get the oid and name for the database being created
        pr_map = pg_database->ProjectionMapForOids(GetOidsForRedoRecord(pg_database, next_redo_record));
        NOISEPAGE_ASSERT(pr_map.find(catalog::postgres::PgDatabase::DATOID.oid_) != pr_map.end(),
                         "PR Map must contain class oid");
        NOISEPAGE_ASSERT(pr_map.find(catalog::postgres::PgDatabase::DATNAME.oid_) != pr_map.end(),
                         "PR Map must contain class name");
        auto next_db_oid = *(reinterpret_cast<catalog::db_oid_t *>(
            next_redo_record->Delta()->AccessWithNullCheck(pr_map[catalog::postgres::PgDatabase::DATOID.oid_])));

        // If the oid matches on the next record, this is a renaming
        if (db_oid == next_db_oid) {
          // Step 4: Extract out the new name
          VarlenEntry name_varlen = *(reinterpret_cast<VarlenEntry *>(
              next_redo_record->Delta()->AccessWithNullCheck(pr_map[catalog::postgres::PgDatabase::DATNAME.oid_])));
          std::string name_string(name_varlen.StringView());

          // Step 5: Rename the database
          auto result UNUSED_ATTRIBUTE =
              catalog_->RenameDatabase(common::ManagedPointer(txn), next_db_oid, name_string);
          NOISEPAGE_ASSERT(result, "Renaming of database should always succeed during replaying");

          // Step 6: Update metadata and clean up additional record processed. We need to use the indexes on
          // pg_database to find what tuple slot we just inserted into. We get the new tuple slot using the oid
          // index.
          auto pg_database_oid_index = catalog_->databases_oid_index_;
          pr_init = pg_database_oid_index->GetProjectedRowInitializer();
          buffer = common::AllocationUtil::AllocateAligned(pr_init.ProjectedRowSize());
          pr = pr_init.InitializeRow(buffer);
          *(reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(0))) = next_db_oid.UnderlyingValue();
          std::vector<TupleSlot> tuple_slot_result;
          pg_database_oid_index->ScanKey(*txn, *pr, &tuple_slot_result);
          NOISEPAGE_ASSERT(tuple_slot_result.size() == 1, "Index scan should only yield one result");
          tuple_slot_map_[next_redo_record->GetTupleSlot()] = tuple_slot_result[0];
          delete[] buffer;
          tuple_slot_map_.erase(delete_record->GetTupleSlot());
          delete[] reinterpret_cast<byte *>(next_redo_record);

          return 1;  // We processed an additional record
        }
      }
    }
  }

  // Step 3: If it wasn't a renaming, we simply need to drop the database
  auto result UNUSED_ATTRIBUTE = catalog_->DeleteDatabase(common::ManagedPointer(txn), db_oid);
  NOISEPAGE_ASSERT(result, "Database deletion should succeed");

  // Step 4: Clean up any metadata
  tuple_slot_map_.erase(delete_record->GetTupleSlot());
  return 0;  // No additional logs processed
}

uint32_t RecoveryManager::ProcessSpecialCasePGClassRecord(
    noisepage::transaction::TransactionContext *txn,
    std::vector<std::pair<noisepage::storage::LogRecord *, std::vector<noisepage::byte *>>> *buffered_changes,
    uint32_t start_idx) {
  auto *curr_record = buffered_changes->at(start_idx).first;
  if (curr_record->RecordType() == LogRecordType::REDO) {
    auto *redo_record = curr_record->GetUnderlyingRecordBodyAs<RedoRecord>();
    NOISEPAGE_ASSERT(redo_record->GetTableOid() == catalog::postgres::PgClass::CLASS_TABLE_OID,
                     "This function must be only called with records modifying pg_class");

    NOISEPAGE_ASSERT(!IsInsertRecord(redo_record), "Special case pg_class record should only be updates");
    auto db_catalog = GetDatabaseCatalog(txn, redo_record->GetDatabaseOid());

    // Updates to pg_class will happen in the following 3 cases:
    //  1. If we update the next col oid. In this case, we don't need to do anything special, just apply the update
    //  2. If we update the schema column, we need to check if this a DDL change (add/drop column). We don't do
    //  anything yet because we don't support DDL changes in the catalog
    //  3. If we update the ptr column, this means we've inserted a new object and we need to recreate the object, and
    //  set the pointer again.
    auto pg_class_ptr = db_catalog->pg_core_.classes_;
    auto redo_record_oids = GetOidsForRedoRecord(pg_class_ptr, redo_record);
    NOISEPAGE_ASSERT(redo_record_oids.size() == 1, "Updates to pg_class should only touch one column");
    auto updated_pg_class_oid = redo_record_oids[0];

    switch (updated_pg_class_oid.UnderlyingValue()) {
      case (catalog::postgres::PgClass::REL_NEXTCOLOID.oid_.UnderlyingValue()): {  // Case 1
        ReplayRedoRecord(txn, curr_record);
        return 0;  // No additional logs processed
      }

      case (catalog::postgres::PgClass::REL_SCHEMA.oid_.UnderlyingValue()): {  // Case 2
        // TODO(Gus): Add support for recovering DDL changes.
        return 0;  // No additional logs processed
      }

      case (catalog::postgres::PgClass::REL_PTR.oid_.UnderlyingValue()): {  // Case 3
        // An update to the ptr column of pg_class means that we have inserted all necessary metadata into the other
        // catalog tables, and we can now recreate the object
        // Step 1: Get the class oid and kind for the object we're updating
        std::vector<catalog::col_oid_t> col_oids = {catalog::postgres::PgClass::RELOID.oid_,
                                                    catalog::postgres::PgClass::RELKIND.oid_};
        auto pr_init = pg_class_ptr->InitializerForProjectedRow(col_oids);
        auto pr_map = pg_class_ptr->ProjectionMapForOids(col_oids);
        auto *buffer = common::AllocationUtil::AllocateAligned(pr_init.ProjectedRowSize());
        auto *pr = pr_init.InitializeRow(buffer);
        pg_class_ptr->Select(common::ManagedPointer(txn), GetTupleSlotMapping(redo_record->GetTupleSlot()), pr);
        auto class_oid =
            *(reinterpret_cast<uint32_t *>(pr->AccessWithNullCheck(pr_map[catalog::postgres::PgClass::RELOID.oid_])));
        auto class_kind = *(reinterpret_cast<catalog::postgres::PgClass::RelKind *>(
            pr->AccessWithNullCheck(pr_map[catalog::postgres::PgClass::RELKIND.oid_])));

        switch (class_kind) {
          case (catalog::postgres::PgClass::RelKind::REGULAR_TABLE): {
            // Step 2: Query pg_attribute for the columns of the table
            auto schema_cols =
                db_catalog->GetColumns<catalog::Schema::Column, catalog::table_oid_t, catalog::col_oid_t>(
                    common::ManagedPointer(txn), catalog::table_oid_t(class_oid));

            // Step 3: Create and set schema in catalog
            auto *schema = new catalog::Schema(std::move(schema_cols));
            bool result UNUSED_ATTRIBUTE = db_catalog->SetTableSchemaPointer<RecoveryManager>(
                common::ManagedPointer(txn), catalog::table_oid_t(class_oid), schema);
            NOISEPAGE_ASSERT(result,
                             "Setting table schema pointer should succeed, entry should be in pg_class already");

            // Step 4: Create and set table pointers in catalog
            storage::SqlTable *sql_table;
            if (class_oid < catalog::START_OID) {  // All catalog tables/indexes have OIDS less than START_OID
              // Use of the -> operator is ok here, since we are the ones who wrapped the table with the ManagedPointer
              sql_table = GetSqlTable(txn, redo_record->GetDatabaseOid(), catalog::table_oid_t(class_oid)).operator->();
            } else {
              sql_table = new SqlTable(block_store_, *schema);
            }
            result =
                db_catalog->SetTablePointer(common::ManagedPointer(txn), catalog::table_oid_t(class_oid), sql_table);
            NOISEPAGE_ASSERT(result, "Setting table pointer should succeed, entry should be in pg_class already");

            // Step 5: Update catalog oid
            db_catalog->UpdateNextOid(class_oid);

            delete[] buffer;
            return 0;  // No additional records processed
          }

          case (catalog::postgres::PgClass::RelKind::INDEX): {
            // Step 2: Query pg_attribute for the columns of the index
            auto index_cols =
                db_catalog->GetColumns<catalog::IndexSchema::Column, catalog::index_oid_t, catalog::indexkeycol_oid_t>(
                    common::ManagedPointer(txn), catalog::index_oid_t(class_oid));

            // Step 3: Query pg_index for the metadata we need for the index schema
            auto pg_indexes_index = db_catalog->pg_core_.indexes_oid_index_;
            pr = pg_indexes_index->GetProjectedRowInitializer().InitializeRow(buffer);
            *(reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(0))) = class_oid;
            std::vector<TupleSlot> tuple_slot_result;
            pg_indexes_index->ScanKey(*txn, *pr, &tuple_slot_result);
            NOISEPAGE_ASSERT(tuple_slot_result.size() == 1, "Index scan should yield one result");

            // NOLINTNEXTLINE
            col_oids.clear();
            col_oids = {catalog::postgres::PgIndex::INDISUNIQUE.oid_, catalog::postgres::PgIndex::INDISPRIMARY.oid_,
                        catalog::postgres::PgIndex::INDISEXCLUSION.oid_, catalog::postgres::PgIndex::INDIMMEDIATE.oid_,
                        catalog::postgres::PgIndex::IND_TYPE.oid_};
            auto pg_index_pr_init = db_catalog->pg_core_.indexes_->InitializerForProjectedRow(col_oids);
            auto pg_index_pr_map = db_catalog->pg_core_.indexes_->ProjectionMapForOids(col_oids);
            delete[] buffer;  // Delete old buffer, it won't be large enough for this PR
            buffer = common::AllocationUtil::AllocateAligned(pg_index_pr_init.ProjectedRowSize());
            pr = pg_index_pr_init.InitializeRow(buffer);
            bool result UNUSED_ATTRIBUTE =
                db_catalog->pg_core_.indexes_->Select(common::ManagedPointer(txn), tuple_slot_result[0], pr);
            NOISEPAGE_ASSERT(result, "Select into pg_index should succeed during recovery");
            bool is_unique = *(reinterpret_cast<bool *>(
                pr->AccessWithNullCheck(pg_index_pr_map[catalog::postgres::PgIndex::INDISUNIQUE.oid_])));
            bool is_primary = *(reinterpret_cast<bool *>(
                pr->AccessWithNullCheck(pg_index_pr_map[catalog::postgres::PgIndex::INDISPRIMARY.oid_])));
            bool is_exclusion = *(reinterpret_cast<bool *>(
                pr->AccessWithNullCheck(pg_index_pr_map[catalog::postgres::PgIndex::INDISEXCLUSION.oid_])));
            bool is_immediate = *(reinterpret_cast<bool *>(
                pr->AccessWithNullCheck(pg_index_pr_map[catalog::postgres::PgIndex::INDIMMEDIATE.oid_])));
            storage::index::IndexType index_type = *(reinterpret_cast<storage::index::IndexType *>(
                pr->AccessWithNullCheck(pg_index_pr_map[catalog::postgres::PgIndex::IND_TYPE.oid_])));

            // Step 4: Create and set IndexSchema in catalog
            auto *index_schema =
                new catalog::IndexSchema(index_cols, index_type, is_unique, is_primary, is_exclusion, is_immediate);
            result = db_catalog->SetIndexSchemaPointer<RecoveryManager>(common::ManagedPointer(txn),
                                                                        catalog::index_oid_t(class_oid), index_schema);
            NOISEPAGE_ASSERT(result,
                             "Setting index schema pointer should succeed, entry should be in pg_class already");

            // Step 5: Create and set index pointer in catalog
            common::ManagedPointer<storage::index::Index> index;
            if (class_oid < catalog::START_OID) {  // All catalog tables/indexes have OIDS less than START_OID
              index = GetCatalogIndex(catalog::index_oid_t(class_oid), db_catalog);
            } else {
              index = common::ManagedPointer(index::IndexBuilder().SetKeySchema(*index_schema).Build());
            }
            result =
                db_catalog->SetIndexPointer(common::ManagedPointer(txn), catalog::index_oid_t(class_oid), index.Get());
            NOISEPAGE_ASSERT(result, "Setting index pointer should succeed, entry should be in pg_class already");

            // Step 6: Update catalog oid
            db_catalog->UpdateNextOid(class_oid);

            delete[] buffer;
            return 0;
          }

          default:
            throw std::runtime_error("Only support recovery of regular tables and indexes");
        }
      }

      default:
        throw std::runtime_error("Unexpected oid updated during replay of update to pg_class");
    }
  }

  // A delete into pg_class has two special cases. Either we are deleting an object, or renaming a table (we assume
  // you can't rename an index). For it to be a table rename, a lot of special conditions need to be true:
  //  1. There is at least one additional record following the delete record
  //  2. This additional record is a redo
  //  3. This additional record is a redo into the same database and pg_class, and is an insert record
  //  4. This additional record inserts an object with the same OID as the one that was being deleted by the redo
  //  record
  // The above conditions are documented in the code below
  // If any of the above conditions are not true, then this is just a drop table/index.
  NOISEPAGE_ASSERT(curr_record->RecordType() == LogRecordType::DELETE, "Must be delete record at this point");
  auto *delete_record = curr_record->GetUnderlyingRecordBodyAs<DeleteRecord>();

  // Step 1: Determine the object oid and type that is being deleted
  auto db_catalog_ptr = GetDatabaseCatalog(txn, delete_record->GetDatabaseOid());
  const auto pg_class = common::ManagedPointer<storage::SqlTable>(db_catalog_ptr->pg_core_.classes_);
  std::vector<catalog::col_oid_t> col_oids = {catalog::postgres::PgClass::RELOID.oid_,
                                              catalog::postgres::PgClass::RELKIND.oid_};
  auto pr_init = pg_class->InitializerForProjectedRow(col_oids);
  auto pr_map = pg_class->ProjectionMapForOids(col_oids);
  auto *buffer = common::AllocationUtil::AllocateAligned(pr_init.ProjectedRowSize());
  auto *pr = pr_init.InitializeRow(buffer);
  pg_class->Select(common::ManagedPointer(txn), GetTupleSlotMapping(delete_record->GetTupleSlot()), pr);
  auto class_oid =
      *(reinterpret_cast<uint32_t *>(pr->AccessWithNullCheck(pr_map[catalog::postgres::PgClass::RELOID.oid_])));
  auto class_kind = *(reinterpret_cast<catalog::postgres::PgClass::RelKind *>(
      pr->AccessWithNullCheck(pr_map[catalog::postgres::PgClass::RELKIND.oid_])));
  delete[] buffer;

  // Step 2: We need to handle the case where we are just renaming a table, in this case we don't wan't to delete
  // the table object. A rename appears as a delete followed by an insert with the same OID.
  if (start_idx + 1 < buffered_changes->size()) {  // Condition 1: there is one more record
    auto *next_record = buffered_changes->at(start_idx + 1).first;
    if (next_record->RecordType() == LogRecordType::REDO) {  // Condition 2: next record is a redo record
      auto *next_redo_record = next_record->GetUnderlyingRecordBodyAs<RedoRecord>();
      if (next_redo_record->GetDatabaseOid() == delete_record->GetDatabaseOid() &&
          next_redo_record->GetTableOid() == delete_record->GetTableOid() &&
          IsInsertRecord(next_redo_record)) {  // Condition 3: next record is an insert into the same pg_class
        // Step 3: Get the oid and kind of the object being inserted
        auto pr_map = pg_class->ProjectionMapForOids(GetOidsForRedoRecord(pg_class, next_redo_record));
        NOISEPAGE_ASSERT(pr_map.find(catalog::postgres::PgClass::RELOID.oid_) != pr_map.end(),
                         "PR Map must contain class oid");
        NOISEPAGE_ASSERT(pr_map.find(catalog::postgres::PgClass::RELNAME.oid_) != pr_map.end(),
                         "PR Map must contain class name");
        NOISEPAGE_ASSERT(pr_map.find(catalog::postgres::PgClass::RELKIND.oid_) != pr_map.end(),
                         "PR Map must contain class kind");
        auto next_class_oid = *(reinterpret_cast<uint32_t *>(
            next_redo_record->Delta()->AccessWithNullCheck(pr_map[catalog::postgres::PgClass::RELOID.oid_])));
        auto next_class_kind UNUSED_ATTRIBUTE = *(reinterpret_cast<catalog::postgres::PgClass::RelKind *>(
            next_redo_record->Delta()->AccessWithNullCheck(pr_map[catalog::postgres::PgClass::RELKIND.oid_])));

        if (class_oid == next_class_oid) {  // Condition 4: If the oid matches on the next record, this is a renaming
          NOISEPAGE_ASSERT(
              class_kind == catalog::postgres::PgClass::RelKind::REGULAR_TABLE && class_kind == next_class_kind,
              "We only allow renaming of tables");
          // Step 4: Extract out the new name
          VarlenEntry name_varlen = *(reinterpret_cast<VarlenEntry *>(
              next_redo_record->Delta()->AccessWithNullCheck(pr_map[catalog::postgres::PgClass::RELNAME.oid_])));
          std::string name_string(name_varlen.StringView());

          // Step 5: Rename the table
          auto result UNUSED_ATTRIBUTE =
              GetDatabaseCatalog(txn, next_redo_record->GetDatabaseOid())
                  ->RenameTable(common::ManagedPointer(txn), catalog::table_oid_t(next_class_oid), name_string);
          NOISEPAGE_ASSERT(result, "Renaming should always succeed during replaying");

          // Step 6: Update metadata and clean up additional record processed. We need to use the indexes on
          // pg_class to find what tuple slot we just inserted into. We get the new tuple slot using the oid index.
          auto pg_class_oid_index =
              GetDatabaseCatalog(txn, next_redo_record->GetDatabaseOid())->pg_core_.classes_oid_index_;
          auto pr_init = pg_class_oid_index->GetProjectedRowInitializer();
          buffer = common::AllocationUtil::AllocateAligned(pr_init.ProjectedRowSize());
          pr = pr_init.InitializeRow(buffer);
          *(reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(0))) = next_class_oid;
          std::vector<TupleSlot> tuple_slot_result;
          pg_class_oid_index->ScanKey(*txn, *pr, &tuple_slot_result);
          NOISEPAGE_ASSERT(tuple_slot_result.size() == 1, "Index scan should only yield one result");
          tuple_slot_map_[next_redo_record->GetTupleSlot()] = tuple_slot_result[0];
          delete[] buffer;
          tuple_slot_map_.erase(delete_record->GetTupleSlot());
          delete[] reinterpret_cast<byte *>(next_redo_record);

          return 1;  // We processed an additional record
        }
      }
    }
  }

  // Step 3: If it was not a renaming, we call to the catalog to delete the class object
  bool result UNUSED_ATTRIBUTE;
  switch (class_kind) {
    case (catalog::postgres::PgClass::RelKind::REGULAR_TABLE): {
      result = db_catalog_ptr->DeleteTable(common::ManagedPointer(txn), catalog::table_oid_t(class_oid));
      break;
    }
    case (catalog::postgres::PgClass::RelKind::INDEX): {
      result = db_catalog_ptr->DeleteIndex(common::ManagedPointer(txn), catalog::index_oid_t(class_oid));
      break;
    }

    default:
      throw std::runtime_error("Only support recovery of drop table and index");
  }

  NOISEPAGE_ASSERT(result, "Table/index DROP should always succeed");

  // Step 5: Clean up metadata
  tuple_slot_map_.erase(delete_record->GetTupleSlot());

  return 0;  // No additional logs processed
}

common::ManagedPointer<storage::SqlTable> RecoveryManager::GetSqlTable(transaction::TransactionContext *txn,
                                                                       const catalog::db_oid_t db_oid,
                                                                       const catalog::table_oid_t table_oid) {
  if (table_oid == catalog::postgres::PgDatabase::DATABASE_TABLE_OID) {
    return common::ManagedPointer(catalog_->databases_);
  }

  auto db_catalog_ptr = GetDatabaseCatalog(txn, db_oid);

  common::ManagedPointer<storage::SqlTable> table_ptr = nullptr;

  switch (table_oid.UnderlyingValue()) {
    case (catalog::postgres::PgClass::CLASS_TABLE_OID.UnderlyingValue()): {
      table_ptr = common::ManagedPointer(db_catalog_ptr->pg_core_.classes_);
      break;
    }
    case (catalog::postgres::PgNamespace::NAMESPACE_TABLE_OID.UnderlyingValue()): {
      table_ptr = common::ManagedPointer(db_catalog_ptr->pg_core_.namespaces_);
      break;
    }
    case (catalog::postgres::PgAttribute::COLUMN_TABLE_OID.UnderlyingValue()): {
      table_ptr = common::ManagedPointer(db_catalog_ptr->pg_core_.columns_);
      break;
    }
    case (catalog::postgres::PgConstraint::CONSTRAINT_TABLE_OID.UnderlyingValue()): {
      table_ptr = common::ManagedPointer(db_catalog_ptr->pg_constraint_.constraints_);
      break;
    }
    case (catalog::postgres::PgIndex::INDEX_TABLE_OID.UnderlyingValue()): {
      table_ptr = common::ManagedPointer(db_catalog_ptr->pg_core_.indexes_);
      break;
    }
    case (catalog::postgres::PgType::TYPE_TABLE_OID.UnderlyingValue()): {
      table_ptr = common::ManagedPointer(db_catalog_ptr->pg_type_.types_);
      break;
    }
    case (catalog::postgres::PgLanguage::LANGUAGE_TABLE_OID.UnderlyingValue()): {
      table_ptr = common::ManagedPointer(db_catalog_ptr->pg_language_.languages_);
      break;
    }
    case (catalog::postgres::PgProc::PRO_TABLE_OID.UnderlyingValue()): {
      table_ptr = common::ManagedPointer(db_catalog_ptr->pg_proc_.procs_);
      break;
    }
    case (catalog::postgres::PgStatistic::STATISTIC_TABLE_OID.UnderlyingValue()): {
      table_ptr = common::ManagedPointer(db_catalog_ptr->pg_stat_.statistics_);
      break;
    }
    default:
      table_ptr = db_catalog_ptr->GetTable(common::ManagedPointer(txn), table_oid);
  }

  NOISEPAGE_ASSERT(table_ptr != nullptr, "Table is not in the catalog for the given oid");
  return table_ptr;
}

std::vector<catalog::col_oid_t> RecoveryManager::GetOidsForRedoRecord(
    common::ManagedPointer<storage::SqlTable> sql_table, RedoRecord *record) {
  std::vector<catalog::col_oid_t> result;
  for (uint16_t i = 0; i < record->Delta()->NumColumns(); i++) {
    col_id_t col_id = record->Delta()->ColumnIds()[i];
    // We should ingore the version pointer column, this is a hidden storage layer column
    if (col_id != VERSION_POINTER_COLUMN_ID) {
      result.emplace_back(sql_table->OidForColId(col_id));
    }
  }
  return result;
}

common::ManagedPointer<storage::index::Index> RecoveryManager::GetCatalogIndex(
    const catalog::index_oid_t oid, const common::ManagedPointer<catalog::DatabaseCatalog> &db_catalog) {
  NOISEPAGE_ASSERT((oid.UnderlyingValue()) < catalog::START_OID, "Oid must be a valid catalog oid");

  switch (oid.UnderlyingValue()) {
    case (catalog::postgres::PgNamespace::NAMESPACE_OID_INDEX_OID.UnderlyingValue()): {
      return db_catalog->pg_core_.namespaces_oid_index_;
    }

    case (catalog::postgres::PgNamespace::NAMESPACE_NAME_INDEX_OID.UnderlyingValue()): {
      return db_catalog->pg_core_.namespaces_name_index_;
    }

    case (catalog::postgres::PgClass::CLASS_OID_INDEX_OID.UnderlyingValue()): {
      return db_catalog->pg_core_.classes_oid_index_;
    }

    case (catalog::postgres::PgClass::CLASS_NAME_INDEX_OID.UnderlyingValue()): {
      return db_catalog->pg_core_.classes_name_index_;
    }

    case (catalog::postgres::PgClass::CLASS_NAMESPACE_INDEX_OID.UnderlyingValue()): {
      return db_catalog->pg_core_.classes_namespace_index_;
    }

    case (catalog::postgres::PgIndex::INDEX_OID_INDEX_OID.UnderlyingValue()): {
      return db_catalog->pg_core_.indexes_oid_index_;
    }

    case (catalog::postgres::PgIndex::INDEX_TABLE_INDEX_OID.UnderlyingValue()): {
      return db_catalog->pg_core_.indexes_table_index_;
    }

    case (catalog::postgres::PgAttribute::COLUMN_OID_INDEX_OID.UnderlyingValue()): {
      return db_catalog->pg_core_.columns_oid_index_;
    }

    case (catalog::postgres::PgAttribute::COLUMN_NAME_INDEX_OID.UnderlyingValue()): {
      return db_catalog->pg_core_.columns_name_index_;
    }

    case (catalog::postgres::PgType::TYPE_OID_INDEX_OID.UnderlyingValue()): {
      return db_catalog->pg_type_.types_oid_index_;
    }

    case (catalog::postgres::PgType::TYPE_NAME_INDEX_OID.UnderlyingValue()): {
      return db_catalog->pg_type_.types_name_index_;
    }

    case (catalog::postgres::PgType::TYPE_NAMESPACE_INDEX_OID.UnderlyingValue()): {
      return db_catalog->pg_type_.types_namespace_index_;
    }

    case (catalog::postgres::PgConstraint::CONSTRAINT_OID_INDEX_OID.UnderlyingValue()): {
      return db_catalog->pg_constraint_.constraints_oid_index_;
    }

    case (catalog::postgres::PgConstraint::CONSTRAINT_NAME_INDEX_OID.UnderlyingValue()): {
      return db_catalog->pg_constraint_.constraints_name_index_;
    }

    case (catalog::postgres::PgConstraint::CONSTRAINT_NAMESPACE_INDEX_OID.UnderlyingValue()): {
      return db_catalog->pg_constraint_.constraints_namespace_index_;
    }

    case (catalog::postgres::PgConstraint::CONSTRAINT_TABLE_INDEX_OID.UnderlyingValue()): {
      return db_catalog->pg_constraint_.constraints_table_index_;
    }

    case (catalog::postgres::PgConstraint::CONSTRAINT_INDEX_INDEX_OID.UnderlyingValue()): {
      return db_catalog->pg_constraint_.constraints_index_index_;
    }

    case (catalog::postgres::PgConstraint::CONSTRAINT_FOREIGNTABLE_INDEX_OID.UnderlyingValue()): {
      return db_catalog->pg_constraint_.constraints_foreigntable_index_;
    }

    case (catalog::postgres::PgLanguage::LANGUAGE_OID_INDEX_OID.UnderlyingValue()): {
      return db_catalog->pg_language_.languages_oid_index_;
    }

    case (catalog::postgres::PgLanguage::LANGUAGE_NAME_INDEX_OID.UnderlyingValue()): {
      return db_catalog->pg_language_.languages_name_index_;
    }

    case (catalog::postgres::PgProc::PRO_OID_INDEX_OID.UnderlyingValue()): {
      return db_catalog->pg_proc_.procs_oid_index_;
    }

    case (catalog::postgres::PgProc::PRO_NAME_INDEX_OID.UnderlyingValue()): {
      return db_catalog->pg_proc_.procs_name_index_;
    }

    case (catalog::postgres::PgStatistic::STATISTIC_OID_INDEX_OID.UnderlyingValue()): {
      return db_catalog->pg_stat_.statistic_oid_index_;
    }

    default:
      throw std::runtime_error("This oid does not belong to any catalog index");
  }
}

uint32_t RecoveryManager::ProcessSpecialCasePGProcRecord(
    noisepage::transaction::TransactionContext *txn,
    std::vector<std::pair<noisepage::storage::LogRecord *, std::vector<noisepage::byte *>>> *buffered_changes,
    uint32_t start_idx) {
  auto *curr_record = buffered_changes->at(start_idx).first;

  if (curr_record->RecordType() == LogRecordType::REDO) {
    auto *redo_record = curr_record->GetUnderlyingRecordBodyAs<RedoRecord>();
    if (IsInsertRecord(redo_record)) {
      ReplayRedoRecord(txn, curr_record);
    } else {
      return 0;
    }
    NOISEPAGE_ASSERT(redo_record->GetTableOid() == catalog::postgres::PgProc::PRO_TABLE_OID,
                     "This function must be only called with records modifying pg_proc");

    // An insert into pg_proc is a special case because we need the catalog to actually create the necessary
    // database catalog objects
    NOISEPAGE_ASSERT(IsInsertRecord(redo_record), "Special case on pg_proc should only be insert");
    common::ManagedPointer<storage::SqlTable> pg_proc =
        catalog_->GetDatabaseCatalog(common::ManagedPointer(txn), redo_record->GetDatabaseOid())->pg_proc_.procs_;
    auto pr_map = pg_proc->ProjectionMapForOids(GetOidsForRedoRecord(pg_proc, redo_record));
    catalog::proc_oid_t proc_oid(*(reinterpret_cast<uint32_t *>(
        redo_record->Delta()->AccessWithNullCheck(pr_map[catalog::postgres::PgProc::PROOID.oid_]))));

    auto result UNUSED_ATTRIBUTE =
        catalog_->GetDatabaseCatalog(common::ManagedPointer(txn), redo_record->GetDatabaseOid())
            ->SetFunctionContextPointer(common::ManagedPointer(txn), proc_oid, nullptr);
    NOISEPAGE_ASSERT(result, "Setting to null did not work");
    return 0;  // No additional records processed
  }
  return 0;
}

const catalog::Schema &RecoveryManager::GetTableSchema(
    transaction::TransactionContext *txn, const common::ManagedPointer<catalog::DatabaseCatalog> &db_catalog,
    const catalog::table_oid_t table_oid) const {
  auto search = catalog_table_schemas_.find(table_oid);
  return (search != catalog_table_schemas_.end()) ? search->second
                                                  : db_catalog->GetSchema(common::ManagedPointer(txn), table_oid);
}

}  // namespace noisepage::storage
