#include "common/macros.h"
#include "storage/checkpoint_manager.h"
#include <vector>
#include <unordered_set>

#define NUM_RESERVED_COLUMNS 1u

namespace terrier::storage {
void CheckpointManager::Checkpoint(const SqlTable &table, const catalog::Schema &schema) {
  std::vector<catalog::col_oid_t> all_col(schema.GetColumns().size());
  uint16_t col_idx = 0;
  for (const catalog::Schema::Column &column : schema.GetColumns()) {
    all_col[col_idx] = column.GetOid();
    col_idx++;
  }

  auto row_pair = table.InitializerForProjectedRow(all_col);
  auto *buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  ProjectedRow *row_buffer = row_pair.first.InitializeRow(buffer);

  for (auto &slot : table) {
    if (table.Select(txn_, slot, row_buffer)) {
      out_.SerializeTuple(row_buffer, &slot, schema, row_pair.second);
    }
  }
  out_.Persist();
  delete[] buffer;
}

void CheckpointManager::Recover(const char *checkpoint_file_path) {
  BufferedTupleReader reader(checkpoint_file_path);

  while (reader.ReadNextBlock()) {
    CheckpointFilePage *page = reader.GetPage();
    // TODO(zhaozhe): check checksum here
    catalog::table_oid_t oid = page->GetTableOid();
    SqlTable *table = GetTable(oid);

    ProjectedRow *row = nullptr;
    while ((row = reader.ReadNextRow()) != nullptr) {
      TupleSlot *slot UNUSED_ATTRIBUTE = reader.ReadNextTupleSlot();
      // loop through columns to deal with non-inlined varlens.
      for (uint16_t projection_list_idx = 0; projection_list_idx < row->NumColumns(); projection_list_idx++) {
        if (!row->IsNull(projection_list_idx)) {
          catalog::col_oid_t col = table->ColOidForId(row->ColumnIds()[projection_list_idx]);
          if (table->GetSchema().GetColumn(col).IsVarlen()) {
            auto *entry = reinterpret_cast<VarlenEntry *>(row->AccessForceNotNull(projection_list_idx));
            if (!entry->IsInlined()) {
              uint32_t varlen_size = reader.ReadNextVarlenSize();
              byte *checkpoint_varlen_content = reader.ReadNextVarlen(varlen_size);
              byte *varlen_content = common::AllocationUtil::AllocateAligned(varlen_size);
              std::memcpy(varlen_content, checkpoint_varlen_content, varlen_size);
              *entry = VarlenEntry::Create(varlen_content, varlen_size, true);
            }
          }
        }
      }
      TupleSlot new_slot = table->Insert(txn_, *row);
      TERRIER_ASSERT(tuple_slot_map_.find(*slot) == tuple_slot_map_.end(),
                     "Any tuple slot during recovery should be encounted only once.");
      tuple_slot_map_[*slot] = new_slot;
    }
  }
}

// TODO(zhaozhes): varlen log is not yet supported in logs.
void CheckpointManager::RecoverFromLogs(const char *log_file_path, terrier::transaction::timestamp_t checkpoint_timestamp) {
  // The recovery algorithm scans the log file twice.
  // On the first pass, a mapping from beginning timestamp (uniquely identifying) to commit timestamp (also uniquely
  // identifying) is established. The purpose of this pass is to know the commit timestamp of each record,
  // so that all commits earlier than the checkpoint can be ignored. Moreover, we can tell if a transaction is aborted
  // if they do not have a commit log record, so it will not appear in the mapping.
  //
  // On the second pass, the valid log records will be replayed sequentially from oldest to newest, because the logging
  // implementation guarantees that the update from the same transaction appears in order, and different transactions
  // appear in commit order.
  
  std::unordered_set<terrier::transaction::timestamp_t> valid_begin_ts;
  
  // First pass
  BufferedLogReader in(log_file_path);
  while (in.HasMore()) {
    LogRecord *log_record = ReadNextLogRecord(&in);
    if (log_record->RecordType() == LogRecordType::COMMIT) {
      TERRIER_ASSERT(valid_begin_ts.find(log_record->TxnBegin()) != valid_begin_ts.end(),
                     "Commit records should be mapped to unique begin timestamps.");
      terrier::transaction::timestamp_t commit_timestamp =
        log_record->GetUnderlyingRecordBodyAs<CommitRecord>()->CommitTime();
      // Only need to recover logs commited after the checkpoint
      if (commit_timestamp > checkpoint_timestamp) {
        valid_begin_ts.insert(log_record->TxnBegin());
      }
    }
    delete[] reinterpret_cast<byte *>(log_record);
  }
  in.Close();
  
  // Second pass
  in = BufferedLogReader(log_file_path);
  while (in.HasMore()) {
    LogRecord *log_record = ReadNextLogRecord(&in);
    if (valid_begin_ts.find(log_record->TxnBegin()) == valid_begin_ts.end()) {
      // This record is from an uncommited transaction or out-of-date transaction.
      delete[] reinterpret_cast<byte *>(log_record);
      continue;
    }
  
    // TODO(zhaozhes): support for multi table. However, the log records stores data_table instead of
    // sql_table, which should be modified I think, so I think we should not currently use the API from log records.
    // For the above reasons, we currently can only support one table recovery, hard-coded as oid 0.
    SqlTable *table = GetTable(static_cast<catalog::table_oid_t>(0));
    if (log_record->RecordType() == LogRecordType::DELETE) {
      auto *delete_record = log_record->GetUnderlyingRecordBodyAs<storage::DeleteRecord>();
      TupleSlot slot = tuple_slot_map_[delete_record->GetTupleSlot()];
      if (!table->Delete(txn_, slot)) {
        TERRIER_ASSERT(0, "Delete failed during log recovery");
      }
    } else if (log_record->RecordType() == LogRecordType::REDO) {
      auto *redo_record = log_record->GetUnderlyingRecordBodyAs<storage::RedoRecord>();
      TERRIER_ASSERT(tuple_slot_map_.find(redo_record->GetTupleSlot()) != tuple_slot_map_.end(),
                     "Tuple slot in a log record should have appeared in checkpoints");
      // Looks like hack here and requires scrutiny once implementation changes.
      // Under current circumstances, if a tuple slot is not seen before in a checkpointed table,
      // then we reason it to be an insert record, otherwise an update record.
      TupleSlot old_slot = redo_record->GetTupleSlot();
      ProjectedRow *row = redo_record->Delta();
      if (tuple_slot_map_.find(old_slot) != tuple_slot_map_.end()) {
        // For an insert record, we have to add its tuple slot into the mapping as well,
        // to cope with a possible later update on it.
        TupleSlot slot = table->Insert(txn_, *row);
        tuple_slot_map_[old_slot] = slot;
      } else {
        TupleSlot slot = tuple_slot_map_[redo_record->GetTupleSlot()];
        if (!table->Update(txn_, slot, *row)) {
          TERRIER_ASSERT(0, "Update failed during log recovery");
        }
      }
      
    }
    delete[] reinterpret_cast<byte *>(log_record);
  }
}

}  // namespace terrier::storage
