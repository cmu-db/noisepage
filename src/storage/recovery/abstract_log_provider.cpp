#include "storage/recovery/abstract_log_provider.h"

#include <utility>
#include <vector>

#include "storage/projected_row.h"

namespace noisepage::storage {

std::pair<LogRecord *, std::vector<byte *>> AbstractLogProvider::ReadNextRecord() {
  // Pointer to buffers for non-aligned varlen entries so we can clean them up down the road
  std::vector<byte *> varlen_contents;
  // Read in LogRecord header data
  auto size = ReadValue<uint32_t>();
  byte *buf = common::AllocationUtil::AllocateAligned(size);
  auto record_type = ReadValue<storage::LogRecordType>();
  auto txn_begin = ReadValue<transaction::timestamp_t>();

  switch (record_type) {
    case (storage::LogRecordType::COMMIT): {
      auto txn_commit = ReadValue<transaction::timestamp_t>();
      auto oldest_active_txn = ReadValue<transaction::timestamp_t>();
      NOISEPAGE_ASSERT(oldest_active_txn != transaction::INVALID_TXN_TIMESTAMP,
                       "INVALID_TXN_TIMESTAMP indicates this was a read only txn, which should "
                       "never have been flushed to disk/network");
      // Okay to fill in null since nobody will invoke the callback.
      // is_read_only argument is set to false, because we do not write out a commit record for a transaction if it is
      // not read-only.
      return {storage::CommitRecord::Initialize(buf, txn_begin, txn_commit, nullptr, nullptr, oldest_active_txn, false,
                                                nullptr, nullptr),
              varlen_contents};
    }

    case (storage::LogRecordType::ABORT): {
      return {storage::AbortRecord::Initialize(buf, txn_begin, nullptr, nullptr), varlen_contents};
    }

    case (storage::LogRecordType::DELETE): {
      auto database_oid = ReadValue<catalog::db_oid_t>();
      auto table_oid = ReadValue<catalog::table_oid_t>();
      auto tuple_slot = ReadValue<storage::TupleSlot>();
      return {storage::DeleteRecord::Initialize(buf, txn_begin, database_oid, table_oid, tuple_slot), varlen_contents};
    }

    case (storage::LogRecordType::REDO): {
      auto database_oid = ReadValue<catalog::db_oid_t>();
      auto table_oid = ReadValue<catalog::table_oid_t>();
      auto tuple_slot = ReadValue<storage::TupleSlot>();

      // TODO(Gus, PR #468): Future addition of checksums should validate these values in case of data corruption.
      auto num_cols = ReadValue<uint16_t>();
      if (num_cols > common::Constants::MAX_COL) {
        throw std::runtime_error("Number of columns deserialized exceeds max columns. possible data corrution");
      }

      // Read in col_ids
      // IDs read individually since we can't guarantee memory layout of vector
      std::vector<storage::col_id_t> col_ids;
      col_ids.reserve(num_cols);
      for (uint16_t i = 0; i < num_cols; i++) {
        const auto col_id = ReadValue<storage::col_id_t>();
        col_ids.push_back(col_id);
      }

      // Read in attribute size boundaries
      std::vector<uint16_t> attr_size_boundaries;
      attr_size_boundaries.reserve(NUM_ATTR_BOUNDARIES);
      for (uint16_t i = 0; i < NUM_ATTR_BOUNDARIES; i++) {
        attr_size_boundaries.push_back(ReadValue<uint16_t>());
      }

      // Compute attr sizes
      std::vector<uint16_t> attr_sizes;
      attr_sizes.reserve(num_cols);
      for (uint16_t attr_idx = 0; attr_idx < num_cols; attr_idx++) {
        attr_sizes.push_back(StorageUtil::AttrSizeFromBoundaries(attr_size_boundaries, attr_idx));
      }

      // Initialize the redo record.
      auto initializer = storage::ProjectedRowInitializer::Create(attr_sizes, col_ids);
      auto *result = storage::RedoRecord::Initialize(buf, txn_begin, database_oid, table_oid, initializer);
      auto *record_body = result->GetUnderlyingRecordBodyAs<RedoRecord>();
      record_body->SetTupleSlot(tuple_slot);
      auto *delta = record_body->Delta();
      NOISEPAGE_ASSERT(delta->NumColumns() == num_cols,
                       "ProjectedRow must have same number of columns as what was serialized.");

      // Get an in memory copy of the record's null bitmap. Note: this is used to guide how the rest of the log file is
      // read in. It doesn't populate the delta's bitmap yet. This will happen naturally as we proceed column-by-column.
      auto bitmap_num_bytes = common::RawBitmap::SizeInBytes(num_cols);
      auto *bitmap_buffer = new uint8_t[bitmap_num_bytes];
      Read(bitmap_buffer, bitmap_num_bytes);
      auto *bitmap = reinterpret_cast<common::RawBitmap *>(bitmap_buffer);

      for (uint16_t i = 0; i < num_cols; i++) {
        if (!bitmap->Test(i)) {
          // Recall that 0 means null in our definition of a ProjectedRow's null bitmap.
          delta->SetNull(i);
          continue;
        }

        // The column is not null, so set the bitmap accordingly and get access to the column value.
        auto *column_value_address = delta->AccessForceNotNull(i);
        // Need to mask off sign bit from VARLEN_COLUMN to get the varlen size
        if (attr_sizes[i] == AttrSizeBytes(VARLEN_COLUMN)) {
          // Read how many bytes this varlen actually is.
          const auto varlen_attribute_size = ReadValue<uint32_t>();

          // Create the varlen entry depending on whether it can be inlined or not
          storage::VarlenEntry varlen_entry;
          if (varlen_attribute_size <= storage::VarlenEntry::InlineThreshold()) {
            // Because it's inline, we can just read it into a stack object, as the varlen constructor will memcpy it
            byte varlen_attribute_content[varlen_attribute_size];
            Read(&varlen_attribute_content, varlen_attribute_size);
            varlen_entry = storage::VarlenEntry::CreateInline(varlen_attribute_content, varlen_attribute_size);
          } else {
            // Allocate a varlen buffer of this many bytes.
            auto *varlen_attribute_content = common::AllocationUtil::AllocateAligned(varlen_attribute_size);
            // Fill the entry with the next bytes from the log file.
            Read(varlen_attribute_content, varlen_attribute_size);

            varlen_entry = storage::VarlenEntry::Create(varlen_attribute_content, varlen_attribute_size, true);
            varlen_contents.push_back(varlen_attribute_content);
          }
          // The attribute value in the ProjectedRow will be a pointer to this varlen entry.
          auto *dest = reinterpret_cast<storage::VarlenEntry *>(column_value_address);
          // Set the value to be the address of the varlen_entry.
          *dest = varlen_entry;
          // Store reference to varlen content to clean up incase of abort
        } else {
          // For inlined attributes, just directly read into the ProjectedRow.
          Read(column_value_address, attr_sizes[i]);
        }
      }

      // Free the memory allocated for the bitmap.
      delete[] bitmap_buffer;
      return {result, std::move(varlen_contents)};
    }

    default:
      throw std::runtime_error("Unknown log record type during deserialization: " +
                               std::to_string(static_cast<uint8_t>(record_type)));
  }
}
}  // namespace noisepage::storage
