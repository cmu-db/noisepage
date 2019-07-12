#include <vector>

#include "storage/recovery/recovery_manager.h"

#include "storage/write_ahead_log/log_io.h"
namespace terrier::storage {

uint32_t RecoveryManager::RecoverFromLogs() {
  // Replay logs until the log provider no longer gives us logs
  uint32_t txns_replayed = 0;
  while (true) {
    auto pair = log_provider_->GetNextRecord();
    auto *log_record = pair.first;

    if (log_record == nullptr) break;

    // If the record is a commit or abort, we replay it, which will replay all its buffered records. Otherwise, we
    // buffer the record.
    if (log_record->RecordType() == LogRecordType::COMMIT || log_record->RecordType() == LogRecordType::ABORT) {
      TERRIER_ASSERT(pair.second.empty(), "Commit or Abort records should not have any varlen pointers");
      if (log_record->RecordType() == LogRecordType::COMMIT) txns_replayed++;
      ReplayTransaction(log_record);
    } else {
      buffered_changes_map_[log_record->TxnBegin()].push_back(pair);
    }
  }
  TERRIER_ASSERT(buffered_changes_map_.empty(), "All buffered changes should have been processed");
  return txns_replayed;
}

void RecoveryManager::ReplayTransaction(LogRecord *log_record) {
  TERRIER_ASSERT(log_record->RecordType() == LogRecordType::COMMIT || log_record->RecordType() == LogRecordType::ABORT,
                 "Records should only be replayed when a commit or abort record is seen");

  // If we are aborting, we can free and discard all buffered changes. Nothing needs to be replayed
  // We flag this as unlikely as its unlikely that abort records will be flushed to disk
  if (unlikely_branch(log_record->RecordType() == LogRecordType::ABORT)) {
    for (auto buffered_pair : buffered_changes_map_[log_record->TxnBegin()]) {
      delete[] reinterpret_cast<byte *>(buffered_pair.first);
      for (auto *entry : buffered_pair.second) {
        delete[] entry;
      }
    }
    buffered_changes_map_.erase(log_record->TxnBegin());
  } else {
    TERRIER_ASSERT(log_record->RecordType() == LogRecordType::COMMIT, "Should only replay when we see a commit record");
    // Begin a txn to replay changes with.
    auto *txn = txn_manager_->BeginTransaction();

    // Apply all buffered changes. They should all succeed. After applying we can safely delete the record
    for (uint32_t i = 0; i < buffered_changes_map_[log_record->TxnBegin()].size(); i++) {
      bool result UNUSED_ATTRIBUTE = true;
      auto *buffered_record = buffered_changes_map_[log_record->TxnBegin()][i].first;

      if (buffered_record->RecordType() == LogRecordType::DELETE) {
        auto *delete_record = buffered_record->GetUnderlyingRecordBodyAs<DeleteRecord>();

        // Get tuple slot
        TERRIER_ASSERT(tuple_slot_map_.find(delete_record->GetTupleSlot()) != tuple_slot_map_.end(),
                       "Tuple slot must already be mapped if Delete record is found");
        auto new_tuple_slot = tuple_slot_map_[delete_record->GetTupleSlot()];

        // Delete the tuple
        auto sql_table_ptr = GetSqlTable(txn, delete_record->GetDatabaseOid(), delete_record->GetTableOid());
        // Stage the delete. This way the recovery operation is logged if logging is enabled
        txn->StageDelete(delete_record->GetDatabaseOid(), delete_record->GetTableOid(), new_tuple_slot);
        UpdateIndexesOnTable(txn, delete_record->GetDatabaseOid(), delete_record->GetTableOid(), new_tuple_slot,
                             false /* delete */);
        result = sql_table_ptr->Delete(txn, new_tuple_slot);
        // We can delete the TupleSlot from the map
        tuple_slot_map_.erase(delete_record->GetTupleSlot());
      } else {
        TERRIER_ASSERT(buffered_record->RecordType() == LogRecordType::REDO, "Must be a redo record");
        auto *redo_record = buffered_record->GetUnderlyingRecordBodyAs<RedoRecord>();

        // Search the map for the tuple slot. If the tuple slot is not in the map, then we have not seen this tuple slot
        // before, so this record is an Insert. If we have seen it before, then the record is an Update
        auto *sql_table = GetSqlTable(txn, redo_record->GetDatabaseOid(), redo_record->GetTableOid());
        auto search = tuple_slot_map_.find(redo_record->GetTupleSlot());
        if (search == tuple_slot_map_.end()) {
          // Save the old tuple slot, and reset the tuple slot in the record
          auto old_tuple_slot = redo_record->GetTupleSlot();
          redo_record->SetTupleSlot(TupleSlot(nullptr, 0));
          // Insert will always succeed
          auto new_tuple_slot = sql_table_ptr->Insert(txn, redo_record);
          UpdateIndexesOnTable(txn, redo_record->GetDatabaseOid(), redo_record->GetTableOid(), new_tuple_slot,
                               true /* insert */);
          // Stage the write. This way the recovery operation is logged if logging is enabled.
          // We stage the write after the insert because Insert sets the tuple slot on the redo record, so we need that
          // to happen before we copy the record into the txn redo buffer.
          TERRIER_ASSERT(redo_record->GetTupleSlot() == new_tuple_slot,
                         "Insert should update redo record with new tuple slot");
          txn->StageRecoveryWrite(buffered_record);
          // Create a mapping of the old to new tuple. The new tuple slot should be used for future updates and deletes.
          tuple_slot_map_[old_tuple_slot] = new_tuple_slot;
        } else {
          auto new_tuple_slot = search->second;
          redo_record->SetTupleSlot(new_tuple_slot);
          // Stage the write. This way the recovery operation is logged if logging is enabled
          txn->StageRecoveryWrite(buffered_record);
          result = sql_table_ptr->Update(txn, redo_record);
        }
      }
      TERRIER_ASSERT(result, "Buffered changes should always succeed during commit");
      delete[] reinterpret_cast<byte *>(buffered_record);
    }
    buffered_changes_map_.erase(log_record->TxnBegin());
    // Commit the txn
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }
  delete[] reinterpret_cast<byte *>(log_record);
}

void RecoveryManager::UpdateIndexesOnTable(transaction::TransactionContext *txn, const catalog::db_oid_t db_oid,
                                           const catalog::table_oid_t table_oid, const TupleSlot &tuple_slot,
                                           const bool insert) {
  auto db_catalog_ptr = GetDatabaseCatalog(txn, db_oid);
  auto index_oids = db_catalog_ptr->GetIndexes(txn, table_oid);

  if (index_oids.empty()) return;

  // Fetch all indexes at once so we can get largest PR size we need and reuse buffer
  // TODO(Gus): We can save ourselves a catalog query by passing in the sql table ptr to this function
  auto sql_table_ptr = GetSqlTable(txn, db_oid, table_oid);
  std::vector<common::ManagedPointer<storage::index::Index>> indexes(index_oids.size());
  uint32_t byte_size = 0;
  for (auto &oid : index_oids) {
    auto index_ptr = db_catalog_ptr->GetIndex(txn, oid);
    indexes.push_back(index_ptr);
    // We want to calculate the size of the largest PR we will create
    byte_size = std::max(byte_size, index_ptr->GetProjectedRowInitializer().ProjectedRowSize());
  }
  auto *buffer = common::AllocationUtil::AllocateAligned(byte_size);

  // Delete from each index
  // TODO(Gus): We are going to assume no indexes on expressions below. Having indexes on expressions would require to
  // evaluate expressions and that's a fucking nightmare
  for (auto index : indexes) {
    auto index_pr = index->GetProjectedRowInitializer().InitializeRow(buffer);
    // TODO(Gus): A possible optimization we could do is fetch all attributes we will need at once, instead of calling
    // Select each time. This would just require copying into the PR each time
    sql_table_ptr->Select(txn, tuple_slot, index_pr);
    if (insert) {
      bool result UNUSED_ATTRIBUTE =
          (index->GetConstraintType() == index::ConstraintType::UNIQUE ? index->InsertUnique : index->Insert)(
              txn, *index_pr, tuple_slot);
      TERRIER_ASSERT(result, "Insert into index should always succeed for a committed transaction");
    } else {
      index->Delete(txn, *index_pr, tuple_slot);
    }
  }
}

}  // namespace terrier::storage
