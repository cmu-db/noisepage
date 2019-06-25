#include <vector>

#include "storage/recovery/recovery_manager.h"

#include "storage/write_ahead_log/log_io.h"
namespace terrier::storage {

void RecoveryManager::ReplayRecord(LogRecord *log_record) {
  TERRIER_ASSERT(log_record->RecordType() == LogRecordType::COMMIT || log_record->RecordType() == LogRecordType::ABORT,
  "Records should only be replayed when a commit or abort record is seen");

  // If we are aborting, we can free and discard all buffered changes. Nothing needs to be replayed
  if (log_record->RecordType() == LogRecordType::ABORT) {
    for (auto *buffered_record : buffered_changes_map_[log_record->TxnBegin()]) {
      delete[] reinterpret_cast<byte *>(buffered_record);
    }
    buffered_changes_map_.erase(log_record->TxnBegin());
  } else {
    TERRIER_ASSERT(log_record->RecordType() == LogRecordType::COMMIT, "Should only replay when we see a commit record");
    // Begin a txn to replay changes with
    auto *txn = txn_manager_->BeginTransaction();

    // Apply all buffered changes. They should all succeed. After applying we can safely delete the record
    for (auto *buffered_record : buffered_changes_map_[log_record->TxnBegin()]) {
      bool result UNUSED_ATTRIBUTE = true;

      if (buffered_record->RecordType() == LogRecordType::DELETE) {
        auto *delete_record = buffered_record->GetUnderlyingRecordBodyAs<DeleteRecord>();

        // Get tuple slot
        TERRIER_ASSERT(tuple_slot_map_.find(delete_record->GetTupleSlot()) != tuple_slot_map_.end(),
                       "Tuple slot must already be mapped if Delete record is found");
        auto new_tuple_slot = tuple_slot_map_[delete_record->GetTupleSlot()];

        // Delete the tuple
        auto *sql_table = GetSqlTable(delete_record->GetDatabaseOid(), delete_record->GetTableOid());
        result = sql_table->Delete(txn, new_tuple_slot);
        // We can delete the TupleSlot from the map
        tuple_slot_map_.erase(delete_record->GetTupleSlot());
      } else {
        TERRIER_ASSERT(buffered_record->RecordType() == LogRecordType::REDO, "Must be a redo record");
        auto *redo_record = buffered_record->GetUnderlyingRecordBodyAs<RedoRecord>();

        // Search the map for the tuple slot. If the tuple slot is not in the map, then we have not seen this tuple slot
        // before, so this record is an Insert. If we have seen it before, then the record is an Update
        auto *sql_table = GetSqlTable(redo_record->GetDatabaseOid(), redo_record->GetTableOid());
        auto search = tuple_slot_map_.find(redo_record->GetTupleSlot());
        if (search == tuple_slot_map_.end()) {
          // Save the old tuple slot, and reset the tuple slot in the record
          auto old_tuple_slot = redo_record->GetTupleSlot();
          redo_record->SetTupleSlot(TupleSlot(nullptr, 0));
          // Insert will always succeed
          auto new_tuple_slot = sql_table->Insert(txn, redo_record);
          // Create a mapping of the old to new tuple. The new tuple slot should be used for updates and deletes.
          tuple_slot_map_[old_tuple_slot] = new_tuple_slot;
        } else {
          auto new_tuple_slot = search->second;
          redo_record->SetTupleSlot(new_tuple_slot);
          // We should return the output of update. An update can fail for a write write conflict, but this will be
          // resolved when we see an abort record. The caller of this function will buffer the redo_record if it fails
          result = sql_table->Update(txn, redo_record);
        }
      }
      TERRIER_ASSERT(result, "Buffered changes should always succeed during commit");
      delete[] reinterpret_cast<byte *>(buffered_record);
    }
    buffered_changes_map_.erase(log_record->TxnBegin());
    // Commit the txn
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }
}

void RecoveryManager::RecoverFromLogs() {
  BufferedLogReader in(log_file_path_.c_str());

  // Replay logs until we reach end of the log
  while (in.HasMore()) {
    auto *log_record = ReadNextRecord(&in);

    // If the record is a commit or abort, we replay it, which will replay all its buffered records. Otherwise, we buffer the record.
    if (log_record->RecordType() == LogRecordType::COMMIT ||
        log_record->RecordType() == LogRecordType::ABORT) {
      ReplayRecord(log_record);
    } else {
      buffered_changes_map_[log_record->TxnBegin()].push_back(log_record);
    }
  }
  TERRIER_ASSERT(buffered_changes_map_.empty(), "All buffered changes should have been processed");
}

LogRecord *RecoveryManager::ReadNextRecord(storage::BufferedLogReader *in) {
  // Read in LogRecord header data
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

  if (record_type == storage::LogRecordType::ABORT) {
    return storage::AbortRecord::Initialize(buf, txn_begin, nullptr);
  }

  auto database_oid = in->ReadValue<catalog::db_oid_t>();
  auto table_oid = in->ReadValue<catalog::table_oid_t>();
  auto tuple_slot = in->ReadValue<storage::TupleSlot>();

  if (record_type == storage::LogRecordType::DELETE) {
    return storage::DeleteRecord::Initialize(buf, txn_begin, database_oid, table_oid, tuple_slot);
  }

  // If code path reaches here, we have a REDO record.
  TERRIER_ASSERT(record_type == storage::LogRecordType::REDO, "Unknown record type during test deserialization");

  // Read in col_ids
  // IDs read individually since we can't guarantee memory layout of vector
  auto num_cols = in->ReadValue<uint16_t>();
  std::vector<storage::col_id_t> col_ids(num_cols);
  for (uint16_t i = 0; i < num_cols; i++) {
    const auto col_id = in->ReadValue<storage::col_id_t>();
    col_ids[i] = col_id;
  }

  // Initialize the redo record. Fetch the block layout from the catalog
  // TODO(Gus): Change this line when catalog is brought in
  auto *sql_table = GetSqlTable(database_oid, table_oid);
  auto &block_layout = sql_table->Layout();
  auto initializer = storage::ProjectedRowInitializer::Create(block_layout, col_ids);
  auto *result = storage::RedoRecord::Initialize(buf, txn_begin, database_oid, table_oid, initializer);
  auto *record_body = result->GetUnderlyingRecordBodyAs<RedoRecord>();
  record_body->SetTupleSlot(tuple_slot);
  auto *delta = record_body->Delta();

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
    auto *column_value_address = delta->AccessForceNotNull(i);
    if (block_layout.IsVarlen(col_ids[i])) {
      // Read how many bytes this varlen actually is.
      const auto varlen_attribute_size = in->ReadValue<uint32_t>();
      // Allocate a varlen buffer of this many bytes.
      auto *varlen_attribute_content = common::AllocationUtil::AllocateAligned(varlen_attribute_size);
      // Fill the entry with the next bytes from the log file.
      in->Read(varlen_attribute_content, varlen_attribute_size);
      // Create the varlen entry depending on whether it can be inlined or not
      storage::VarlenEntry varlen_entry;
      if (varlen_attribute_size <= storage::VarlenEntry::InlineThreshold()) {
        varlen_entry = storage::VarlenEntry::CreateInline(varlen_attribute_content, varlen_attribute_size);
      } else {
        varlen_entry = storage::VarlenEntry::Create(varlen_attribute_content, varlen_attribute_size, true);
      }
      // The attribute value in the ProjectedRow will be a pointer to this varlen entry.
      auto *dest = reinterpret_cast<storage::VarlenEntry *>(column_value_address);
      // Set the value to be the address of the varlen_entry.
      *dest = varlen_entry;
    } else {
      // For inlined attributes, just directly read into the ProjectedRow.
      in->Read(column_value_address, block_layout.AttrSize(col_ids[i]));
    }
  }

  // Free the memory allocated for the bitmap.
  delete[] bitmap_buffer;

  return result;
}

}  // namespace terrier::storage
