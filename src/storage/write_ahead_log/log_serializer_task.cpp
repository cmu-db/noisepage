#include "storage/write_ahead_log/log_serializer_task.h"
#include <queue>
#include <utility>
#include "common/scoped_timer.h"
#include "common/thread_context.h"
#include "metrics/metrics_store.h"
#include "transaction/transaction_context.h"

namespace terrier::storage {

void LogSerializerTask::LogSerializerTaskLoop() {
  do {
    std::this_thread::sleep_for(serialization_interval_);
    Process();
  } while (run_task_);
  // To be extra sure we processed everything
  Process();
  TERRIER_ASSERT(flush_queue_.empty(), "Termination of LogSerializerTask should hand off all buffers to consumers");
}

void LogSerializerTask::Process() {
  uint64_t elapsed_ns = 0, num_bytes = 0, num_records = 0;
  {
    common::ScopedTimer<std::chrono::nanoseconds> scoped_timer(&elapsed_ns);
    // In a short critical section, get all buffers to serialize. We move them to a temp queue to reduce contention
    // on the queue transactions interact with
    std::queue<RecordBufferSegment *> temp_flush_queue;
    {
      common::SpinLatch::ScopedSpinLatch guard(&flush_queue_latch_);
      temp_flush_queue = std::move(flush_queue_);
    }
    while (!temp_flush_queue.empty()) {
      RecordBufferSegment *buffer = temp_flush_queue.front();
      temp_flush_queue.pop();

      // Serialize the Redo buffer and release it to the buffer pool
      IterableBufferSegment<LogRecord> task_buffer(buffer);
      const auto num_bytes_and_records = SerializeBuffer(&task_buffer);
      buffer_pool_->Release(buffer);
      num_bytes += num_bytes_and_records.first;
      num_records += num_bytes_and_records.second;
    }

    // Mark the last buffer that was written to as full
    if (filled_buffer_ != nullptr) HandFilledBufferToWriter();
  }
  if (num_bytes > 0 && common::thread_context.metrics_store_ != nullptr) {
    common::thread_context.metrics_store_->RecordSerializerData(elapsed_ns, num_bytes, num_records);
  }
}

/**
 * Used by the serializer thread to get a buffer to serialize data to
 * @return buffer to write to
 */
BufferedLogWriter *LogSerializerTask::GetCurrentWriteBuffer() {
  if (filled_buffer_ == nullptr) {
    empty_buffer_queue_->Dequeue(&filled_buffer_);
  }
  return filled_buffer_;
}

/**
 * Hand over the current buffer and commit callbacks for commit records in that buffer to the log consumer task
 */
void LogSerializerTask::HandFilledBufferToWriter() {
  // Hand over the filled buffer
  filled_buffer_queue_->Enqueue(std::make_pair(filled_buffer_, commits_in_buffer_));
  // Signal disk log consumer task thread that a buffer has been handed over
  disk_log_writer_thread_cv_->notify_one();
  // Mark that the task doesn't have a buffer in its possession to which it can write to
  commits_in_buffer_.clear();
  filled_buffer_ = nullptr;
}

std::pair<uint64_t, uint64_t> LogSerializerTask::SerializeBuffer(
    IterableBufferSegment<LogRecord> *buffer_to_serialize) {
  uint64_t num_bytes = 0, num_records = 0;

  // Iterate over all redo records in the redo buffer through the provided iterator
  for (LogRecord &record : *buffer_to_serialize) {
    if (record.RecordType() == LogRecordType::COMMIT) {
      auto *commit_record = record.GetUnderlyingRecordBodyAs<CommitRecord>();

      // If a transaction is read-only, then the only record it generates is its commit record. This commit record is
      // necessary for the transaction's callback function to be invoked, but there is no need to serialize it, as
      // it corresponds to a transaction with nothing to redo.
      if (!commit_record->IsReadOnly()) num_bytes += SerializeRecord(record);
      commits_in_buffer_.emplace_back(commit_record->Callback(), commit_record->CallbackArg());
      // Not safe to mark read only transactions as the transactions are deallocated preemptively without waiting for
      // logging (there is nothing to log after all)
      if (!commit_record->IsReadOnly()) commit_record->Txn()->log_processed_ = true;
    } else if (record.RecordType() == LogRecordType::ABORT) {
      // If an abort record shows up at all, the transaction cannot be read-only
      num_bytes += SerializeRecord(record);
      record.GetUnderlyingRecordBodyAs<AbortRecord>()->Txn()->log_processed_ = true;
    } else {
      // Any record that is not a commit record is always serialized.`
      num_bytes += SerializeRecord(record);
    }
    num_records++;
  }

  return {num_bytes, num_records};
}

uint64_t LogSerializerTask::SerializeRecord(const terrier::storage::LogRecord &record) {
  uint64_t num_bytes = 0;
  // First, serialize out fields common across all LogRecordType's.

  // Note: This is the in-memory size of the log record itself, i.e. inclusive of padding and not considering the size
  // of any potential varlen entries. It is logically different from the size of the serialized record, which the log
  // manager generates in this function. In particular, the later value is very likely to be strictly smaller when the
  // LogRecordType is REDO. On recovery, the goal is to turn the serialized format back into an in-memory log record of
  // this size.
  num_bytes += WriteValue(record.Size());

  num_bytes += WriteValue(record.RecordType());
  num_bytes += WriteValue(record.TxnBegin());

  switch (record.RecordType()) {
    case LogRecordType::REDO: {
      auto *record_body = record.GetUnderlyingRecordBodyAs<RedoRecord>();
      num_bytes += WriteValue(record_body->GetDatabaseOid());
      num_bytes += WriteValue(record_body->GetTableOid());
      num_bytes += WriteValue(record_body->GetTupleSlot());

      auto *delta = record_body->Delta();
      // Write out which column ids this redo record is concerned with. On recovery, we can construct the appropriate
      // ProjectedRowInitializer from these ids and their corresponding block layout.
      num_bytes += WriteValue(delta->NumColumns());
      num_bytes += WriteValue(delta->ColumnIds(), static_cast<uint32_t>(sizeof(col_id_t)) * delta->NumColumns());

      // Write out the null bitmap.
      num_bytes += WriteValue(&(delta->Bitmap()), common::RawBitmap::SizeInBytes(delta->NumColumns()));

      // We need the block layout to determine the size of each attribute.
      const auto &block_layout = record_body->GetTupleSlot().GetBlock()->data_table_->GetBlockLayout();
      for (uint16_t i = 0; i < delta->NumColumns(); i++) {
        const auto *column_value_address = delta->AccessWithNullCheck(i);
        if (column_value_address == nullptr) {
          // If the column in this REDO record is null, then there's nothing to serialize out. The bitmap contains all
          // the relevant information.
          continue;
        }
        // Get the column id of the current column in the ProjectedRow.
        col_id_t col_id = delta->ColumnIds()[i];

        if (block_layout.IsVarlen(col_id)) {
          // Inline column value is a pointer to a VarlenEntry, so reinterpret as such.
          const auto *varlen_entry = reinterpret_cast<const VarlenEntry *>(column_value_address);
          // Serialize out length of the varlen entry.
          num_bytes += WriteValue(varlen_entry->Size());
          if (varlen_entry->IsInlined()) {
            // Serialize out the prefix of the varlen entry.
            num_bytes += WriteValue(varlen_entry->Prefix(), varlen_entry->Size());
          } else {
            // Serialize out the content field of the varlen entry.
            num_bytes += WriteValue(varlen_entry->Content(), varlen_entry->Size());
          }
        } else {
          // Inline column value is the actual data we want to serialize out.
          // Note that by writing out AttrSize(col_id) bytes instead of just the difference between successive offsets
          // of the delta record, we avoid serializing out any potential padding.
          num_bytes += WriteValue(column_value_address, block_layout.AttrSize(col_id));
        }
      }
      break;
    }
    case LogRecordType::DELETE: {
      auto *record_body = record.GetUnderlyingRecordBodyAs<DeleteRecord>();
      num_bytes += WriteValue(record_body->GetDatabaseOid());
      num_bytes += WriteValue(record_body->GetTableOid());
      num_bytes += WriteValue(record_body->GetTupleSlot());
      break;
    }
    case LogRecordType::COMMIT: {
      num_bytes += WriteValue(record.GetUnderlyingRecordBodyAs<CommitRecord>()->CommitTime());
      break;
    }
    case LogRecordType::ABORT: {
      // AbortRecord does not hold any additional metadata
      break;
    }
  }

  return num_bytes;
}

uint32_t LogSerializerTask::WriteValue(const void *val, const uint32_t size) {
  // Serialize the value and copy it to the buffer
  BufferedLogWriter *out = GetCurrentWriteBuffer();
  uint32_t size_written = 0;

  while (size_written < size) {
    const byte *val_byte = reinterpret_cast<const byte *>(val) + size_written;
    size_written += out->BufferWrite(val_byte, size - size_written);
    if (out->IsBufferFull()) {
      // Mark the buffer full for the disk log consumer task thread to flush it
      HandFilledBufferToWriter();
      // Get an empty buffer for writing this value
      out = GetCurrentWriteBuffer();
    }
  }
  return size;
}

}  // namespace terrier::storage
