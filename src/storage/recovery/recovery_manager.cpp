#include "storage/recovery/recovery_manager.h"

#include "storage/write_ahead_log/log_io.h"

namespace terrier::storage {

bool RecoveryManager::ReplayRecord(LogRecord *log_record) {
  switch (log_record->RecordType()) {
    case (LogRecordType::COMMIT): {
      // If we have buffered changes, apply those changes. They should all succeed. After applying we can safely delete
      // the record
      for (auto *buffered_record : buffered_changes_map_[log_record->TxnBegin()]) {
        bool result UNUSED_ATTRIBUTE = ReplayRecord(buffered_record);
        TERRIER_ASSERT(result, "Buffered changes should succeed during commit");
        delete[] reinterpret_cast<byte *>(buffered_record);
      }
      buffered_changes_map_.erase(log_record->TxnBegin());

      // Commit the txn
      TERRIER_ASSERT(txn_map_.find(log_record->TxnBegin()) != txn_map_.end(),
                     "Txn must already exist if CommitRecord is found");
      txn_manager_->Commit(txn_map_[log_record->TxnBegin()], transaction::TransactionUtil::EmptyCallback, nullptr);
      break;
    }

    case (LogRecordType::ABORT): {
      // If we are aborting, we can discard all buffered changes
      for (auto *buffered_record : buffered_changes_map_[log_record->TxnBegin()]) {
        delete[] reinterpret_cast<byte *>(buffered_record);
      }
      buffered_changes_map_.erase(log_record->TxnBegin());

      // Abort the txn
      TERRIER_ASSERT(txn_map_.find(log_record->TxnBegin()) != txn_map_.end(),
                     "Txn must already exist if AbortRecord is found");
      txn_manager_->Abort(txn_map_[log_record->TxnBegin()]);
      break;
    }

    case (LogRecordType::DELETE): {
      auto *delete_record = log_record->GetUnderlyingRecordBodyAs<DeleteRecord>();

      // Get Txn
      TERRIER_ASSERT(txn_map_.find(log_record->TxnBegin()) != txn_map_.end(),
                     "Txn must already exist if DeleteRecord is found");
      auto txn = txn_map_[log_record->TxnBegin()];
      // Get tuple slot
      TERRIER_ASSERT(tuple_slot_map_.find(delete_record->GetTupleSlot()) != tuple_slot_map_.end(),
                     "Tuple slot must already be mapped if Delete record is found");
      auto new_tuple_slot = tuple_slot_map_[delete_record->GetTupleSlot()];

      // Delete, and return success or not
      auto *sql_table = catalog_->at(delete_record->GetDatabaseOid()).at(delete_record->GetTableOid());
      return sql_table->Delete(txn, new_tuple_slot);
    }

    case (LogRecordType::REDO): {
      auto *redo_record = log_record->GetUnderlyingRecordBodyAs<RedoRecord>();

      // If we haven't seen this txn yet, then begin the transaction
      // TODO(Gus): Replace the following line when timestamp manager is brought in
      if (txn_map_.find(log_record->TxnBegin()) == txn_map_.end()) {
        auto txn = txn_manager_->BeginTransaction(log_record->TxnBegin());
        TERRIER_ASSERT(txn->StartTime() == log_record->TxnBegin(),
                       "Txn timestamp should be the same as logged timestamp");
        txn_map_[txn->StartTime()] = txn;
      }
      auto txn = txn_map_[log_record->TxnBegin()];

      // Search the map for the tuple slot. If the tuple slot is not in the map, then we have not seen this tuple slot
      // before, so this record is an Insert. If we have seen it before, then the record is an Update
      auto *sql_table = catalog_->at(redo_record->GetDatabaseOid()).at(redo_record->GetTableOid());
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
        return sql_table->Update(txn, redo_record);
      }
      break;
    }
  }
  return true;
}

void RecoveryManager::RecoverFromLogs() {
  BufferedLogReader in(log_file_path_.c_str());

  // Replay logs until we reach end of the log
  while (in.HasMore()) {
    auto *log_record = ReadNextRecord(&in);

    // If replaying the record failed, then we need to buffer the record. Buffered records will be deleted when it's txn
    // commits or aborts
    if (!ReplayRecord(log_record)) {
      buffered_changes_map_[log_record->TxnBegin()].push_back(log_record);
    } else {
      // if the replaying succeeded, we can safely delete the record
      delete[] reinterpret_cast<byte *>(log_record);
    }
  }
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
    return storage::AbortRecord::Initialize(buf, txn_begin);
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
  auto block_layout = catalog_->at(database_oid).at(table_oid)->table_.layout;
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
