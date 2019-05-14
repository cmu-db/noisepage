#include "storage/checkpoint_manager.h"
#include <unordered_set>
#include <vector>
#include "common/macros.h"

#define NUM_RESERVED_COLUMNS 1u

namespace terrier::storage {
void CheckpointManager::Checkpoint(const SqlTable &table, const catalog::Schema &schema, uint32_t catalog_type) {
  std::vector<catalog::col_oid_t> all_col(schema.GetColumns().size());
  uint16_t col_idx = 0;
  for (const catalog::Schema::Column &column : schema.GetColumns()) {
    all_col[col_idx] = column.GetOid();
    col_idx++;
  }

  out_.SetTableOid(table.Oid());
  if (catalog_type == 0) {  // normal table
    // TODO(mengyang): should be the version of the table.
    out_.SetVersion(0);
  } else {  // catalog table
    out_.SetVersion(catalog_type);
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
  if (strcmp(checkpoint_file_path, "") == 0) {
    // No valid checkpoints till now. Directly return.
    return;
  }
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
      TERRIER_ASSERT(tuple_slot_map_.find(new_slot) == tuple_slot_map_.end(),
                     "Any tuple slot during recovery should be encountered only once.");
      tuple_slot_map_[*slot] = new_slot;
    }
  }
}

void CheckpointManager::RecoverFromLogs(const char *log_file_path,
                                        terrier::transaction::timestamp_t checkpoint_timestamp) {
  // The recovery algorithm scans the log file twice.
  // On the first pass, a mapping from beginning timestamp (uniquely identifying) to commit timestamp (also uniquely
  // identifying) is established. The purpose of this pass is to know the commit timestamp of each record,
  // so that all commits earlier than the checkpoint can be ignored. Moreover, we can tell if a transaction is aborted
  // if they do not have a commit log record, so it will not appear in the mapping.
  //
  // On the second pass, the valid log records will be replayed sequentially from oldest to newest, because the logging
  // implementation guarantees that the update from the same transaction appears in order, and different transactions
  // appear in commit order.
  //
  // Garbage collection for varlen contents should be careful. If a tuple is inserted or updated into the table, GC
  // will take care of reclaiming varlen contents. Otherwise, we should free them here.

  std::unordered_set<terrier::transaction::timestamp_t> valid_begin_ts;

  // First pass
  // This first pass only needs commit timestamps and delete timestamps. If there is a performance bottleneck here,
  // a special implementation of ReadNextLogRecord can be added to reduce other useless overhead.
  BufferedLogReader in(log_file_path);
  while (in.HasMore()) {
    std::vector<byte *> varlen_contents;
    LogRecord *log_record = ReadNextLogRecord(&in, &varlen_contents);
    if (log_record->RecordType() == LogRecordType::COMMIT) {
      TERRIER_ASSERT(valid_begin_ts.find(log_record->TxnBegin()) == valid_begin_ts.end(),
                     "Commit records should be mapped to unique begin timestamps.");
      terrier::transaction::timestamp_t commit_timestamp =
          log_record->GetUnderlyingRecordBodyAs<CommitRecord>()->CommitTime();
      // Only need to recover logs commited after the checkpoint
      if (commit_timestamp > checkpoint_timestamp) {
        valid_begin_ts.insert(log_record->TxnBegin());
      }
    }
    // Free up all varlen contents created in ReadNextLogRecord.
    for (auto varlen_content : varlen_contents) {
      delete[] varlen_content;
    }
    delete[] reinterpret_cast<byte *>(log_record);
  }
  // No need for in.Close(), because it is already closed log.io when the file has no more contents

  // Second pass
  in = BufferedLogReader(log_file_path);
  while (in.HasMore()) {
    std::vector<byte *> varlen_contents;
    LogRecord *log_record = ReadNextLogRecord(&in, &varlen_contents);
    if (valid_begin_ts.find(log_record->TxnBegin()) == valid_begin_ts.end()) {
      // This record is from an uncommited transaction or out-of-date transaction.
      // Caution: We have to deallocate the varlen content first to prevent memory leak. We do not have to worry
      // about the valid log records, because they will be reclaimed by GC.
      for (auto varlen_content : varlen_contents) {
        delete[] varlen_content;
      }
      delete[] reinterpret_cast<byte *>(log_record);
      continue;
    }

    // TODO(zhaozhes): support for multi table. However, the log records stores data_table instead of
    // sql_table, and always record table oid 0.
    // For this reason, we currently can only support one table recovery, hard-coded as oid 0.
    SqlTable *table = GetTable(static_cast<catalog::table_oid_t>(0));
    if (log_record->RecordType() == LogRecordType::DELETE) {
      auto *delete_record = log_record->GetUnderlyingRecordBodyAs<storage::DeleteRecord>();
      TupleSlot slot = tuple_slot_map_[delete_record->GetTupleSlot()];
      if (!table->Delete(txn_, slot)) {
        TERRIER_ASSERT(0, "Delete failed during log recovery");
      }
    } else if (log_record->RecordType() == LogRecordType::REDO) {
      auto *redo_record = log_record->GetUnderlyingRecordBodyAs<storage::RedoRecord>();
      // Should be careful about the logic here.
      // Under current circumstances, if a tuple slot is not seen before in a checkpointed table,
      // then we reason it to be an insert record, otherwise an update record.
      TupleSlot old_slot = redo_record->GetTupleSlot();
      ProjectedRow *row = redo_record->Delta();
      if (tuple_slot_map_.find(old_slot) == tuple_slot_map_.end()) {
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

storage::LogRecord *CheckpointManager::ReadNextLogRecord(storage::BufferedLogReader *in,
                                                         std::vector<byte *> *varlen_contents) {
  // TODO(Justin): Fit this to new serialization format after it is complete.
  auto size = in->ReadValue<uint32_t>();
  byte *buf = common::AllocationUtil::AllocateAligned(size);
  auto record_type = in->ReadValue<storage::LogRecordType>();
  auto txn_begin = in->ReadValue<transaction::timestamp_t>();
  if (record_type == storage::LogRecordType::COMMIT) {
    auto txn_commit = in->ReadValue<transaction::timestamp_t>();
    // Okay to fill in null since nobody will invoke the callback.
    // is_read_only argument is set to false, because we do not write out a commit record for a transaction if it is
    // not read-only.
    return storage::CommitRecord::Initialize(buf, txn_begin, txn_commit, nullptr, nullptr, false, nullptr);
  }
  auto table_oid = in->ReadValue<catalog::table_oid_t>();
  auto tuple_slot = in->ReadValue<storage::TupleSlot>();
  if (record_type == storage::LogRecordType::DELETE) {
    // TODO(Justin): set a pointer to the correct data table? Will this even be useful for recovery?
    return storage::DeleteRecord::Initialize(buf, txn_begin, nullptr, tuple_slot);
  }
  // If code path reaches here, we have a REDO record.
  SqlTable *table = GetTable(table_oid);
  auto num_cols = in->ReadValue<uint16_t>();

  // TODO(Justin): Could do this with just one read of (sizeof(col_id_t) * num_cols) bytes, and then index in. That's
  //  probably faster, but stick with the more naive way for now. We need a vector, not just a col_id_t[], because
  //  that is what ProjectedRowInitializer needs.
  std::vector<catalog::col_oid_t> col_oids(num_cols);
  for (uint16_t i = 0; i < num_cols; i++) {
    const auto col_id = in->ReadValue<storage::col_id_t>();
    col_oids[i] = table->ColOidForId(col_id);
  }
  // Initialize the redo record.
  auto row_pair = table->InitializerForProjectedRow(col_oids);
  // TODO(Justin): set a pointer to the correct data table? Will this even be useful for recovery?
  auto *result = storage::RedoRecord::Initialize(buf, txn_begin, nullptr, tuple_slot, row_pair.first);
  auto *delta = result->GetUnderlyingRecordBodyAs<storage::RedoRecord>()->Delta();

  // Get an in memory copy of the record's null bitmap. Note: this is used to guide how the rest of the log file is
  // read in. It doesn't populate the delta's bitmap yet. This will happen naturally as we proceed column-by-column.
  auto bitmap_num_bytes = common::RawBitmap::SizeInBytes(num_cols);
  auto *bitmap_buffer = new uint8_t[bitmap_num_bytes];
  in->Read(bitmap_buffer, bitmap_num_bytes);
  auto *bitmap = reinterpret_cast<common::RawBitmap *>(bitmap_buffer);

  for (uint16_t i = 0; i < num_cols; i++) {
    if (!bitmap->Test(i)) {
      // Recall that 0 means null in our definition of a ProjectedRow's null bitmap.
      delta->SetNull(i);
      continue;
    }

    // The column is not null, so set the bitmap accordingly and get access to the column value.
    auto *column_value_address = delta->AccessForceNotNull(row_pair.second.at(col_oids[i]));
    if (table->GetSchema().GetColumn(col_oids[i]).IsVarlen()) {
      // Read how many bytes this varlen actually is.
      const auto varlen_attribute_size = in->ReadValue<uint32_t>();
      // Allocate a varlen entry of this many bytes.
      byte *varlen_content = common::AllocationUtil::AllocateAligned(varlen_attribute_size);
      // Fill the entry with the next bytes from the log file.
      in->Read(varlen_content, varlen_attribute_size);
      // The attribute value in the ProjectedRow will be a pointer to this varlen entry.
      auto *entry = reinterpret_cast<storage::VarlenEntry *>(column_value_address);
      // Set the value to be the address of the varlen_entry.
      if (varlen_attribute_size > VarlenEntry::InlineThreshold()) {
        *entry = storage::VarlenEntry::Create(varlen_content, varlen_attribute_size, true);
        // leave memory to be reclaimed outside, because we do not know whether GC is responsible for this now
        varlen_contents->push_back(varlen_content);
      } else {
        *entry = storage::VarlenEntry::CreateInline(varlen_content, varlen_attribute_size);
        // should free memory for inclined entries, because they are already memcpy-ed into the tuple
        delete[] varlen_content;
      }
    } else {
      // For inlined attributes, just directly read into the ProjectedRow.
      in->Read(column_value_address, table->GetSchema().GetColumn(col_oids[i]).GetAttrSize());
    }
  }

  // Free the memory allocated for the bitmap.
  delete[] bitmap_buffer;

  return result;
}

}  // namespace terrier::storage
