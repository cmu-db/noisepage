#include "storage/write_ahead_log/log_manager.h"
#include <transaction/transaction_context.h>

namespace terrier::storage {
void LogManager::Process() {
  while (true) {
    RecordBufferSegment *buffer;
    // In a short critical section, try to dequeue an item
    {
      common::SpinLatch::ScopedSpinLatch guard(&flush_queue_latch_);
      if (flush_queue_.empty()) break;
      buffer = flush_queue_.front();
      flush_queue_.pop();
    }
    // Serialize the Redo buffer and release it to the buffer pool
    IterableBufferSegment<LogRecord> task_buffer(buffer);
    SerializeBuffer(&task_buffer);
    buffer_pool_->Release(buffer);
  }
  // Mark the last buffer that was written to as full
  if (filled_buffer_ != nullptr) {
    HandFilledBufferToWriter();
  }
  Flush();
}

void LogManager::Flush() {
  {
    std::unique_lock<std::mutex> lock(persist_lock_);
    // Signal the log consumer thread to persist the buffers to disk
    do_persist_ = true;
    consumer_thread_cv_.notify_one();

    // Wait for the log consumer thread to persist the logs
    persist_cv_.wait(lock, [&] { return !do_persist_; });
  }
  // Execute the callbacks for the transactions that have been persisted
  for (auto &callback : commits_in_buffer_) callback.first(callback.second);
  commits_in_buffer_.clear();
}

void LogManager::Shutdown() {
  run_log_manager_ = false;

  // Process will call Flush, which will force the log consumer to consume logs
  Process();

  // Signal the log consumer thread to shutdown
  // This is a blocking call, and will return when the consumer task has shut down
  DedicatedThreadRegistry::GetInstance().StopTask(this,
                                                  common::ManagedPointer<DedicatedThreadTask>(log_consumer_task_));
  TERRIER_ASSERT(filled_buffer_queue_.Empty(), "Consumer should have processed all filled buffers\n");

  // Close the buffers corresponding to the log file
  for (auto buf : buffers_) {
    buf.Close();
  }
  // Clear buffer queues
  BufferedLogWriter *tmp;
  while (!empty_buffer_queue_.Empty()) empty_buffer_queue_.Dequeue(&tmp);
  while (!filled_buffer_queue_.Empty()) filled_buffer_queue_.Dequeue(&tmp);
  buffers_.clear();
}

void LogManager::SerializeBuffer(IterableBufferSegment<LogRecord> *buffer_to_serialize) {
  // Iterate over all redo records in the redo buffer through the provided iterator
  for (LogRecord &record : *buffer_to_serialize) {
    if (record.RecordType() == LogRecordType::COMMIT) {
      auto *commit_record = record.GetUnderlyingRecordBodyAs<CommitRecord>();

      // If a transaction is read-only, then the only record it generates is its commit record. This commit record is
      // necessary for the transaction's callback function to be invoked, but there is no need to serialize it, as
      // it corresponds to a transaction with nothing to redo.
      if (!commit_record->IsReadOnly()) SerializeRecord(record);
      commits_in_buffer_.emplace_back(commit_record->Callback(), commit_record->CallbackArg());
      // Not safe to mark read only transactions as the transactions are deallocated preemptively without waiting for
      // logging (there is nothing to log after all)
      if (!commit_record->IsReadOnly()) commit_record->Txn()->log_processed_ = true;
    } else {
      // Any record that is not a commit record is always serialized.`
      SerializeRecord(record);
    }
  }
}

void LogManager::SerializeRecord(const terrier::storage::LogRecord &record) {
  // First, serialize out fields common across all LogRecordType's.

  // Note: This is the in-memory size of the log record itself, i.e. inclusive of padding and not considering the size
  // of any potential varlen entries. It is logically different from the size of the serialized record, which the log
  // manager generates in this function. In particular, the later value is very likely to be strictly smaller when the
  // LogRecordType is REDO. On recovery, the goal is to turn the serialized format back into an in-memory log record of
  // this size.
  WriteValue(record.Size());

  WriteValue(record.RecordType());
  WriteValue(record.TxnBegin());

  switch (record.RecordType()) {
    case LogRecordType::REDO: {
      auto *record_body = record.GetUnderlyingRecordBodyAs<RedoRecord>();
      WriteValue(record_body->GetTableOid());
      WriteValue(record_body->GetTupleSlot());

      auto *delta = record_body->Delta();
      // Write out which column ids this redo record is concerned with. On recovery, we can construct the appropriate
      // ProjectedRowInitializer from these ids and their corresponding block layout.
      WriteValue(delta->NumColumns());
      WriteValue(delta->ColumnIds(), static_cast<uint32_t>(sizeof(col_id_t)) * delta->NumColumns());

      // Write out the null bitmap.
      WriteValue(&(delta->Bitmap()), common::RawBitmap::SizeInBytes(delta->NumColumns()));

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
          const auto *varlen_entry = reinterpret_cast<const VarlenEntry *>(*column_value_address);
          // Serialize out length of the varlen entry.
          WriteValue(varlen_entry->Size());
          if (varlen_entry->IsInlined()) {
            // Serialize out the prefix of the varlen entry.
            WriteValue(varlen_entry->Prefix(), varlen_entry->Size());
          } else {
            // Serialize out the content field of the varlen entry.
            WriteValue(varlen_entry->Content(), varlen_entry->Size());
          }
        } else {
          // Inline column value is the actual data we want to serialize out.
          // Note that by writing out AttrSize(col_id) bytes instead of just the difference between successive offsets
          // of the delta record, we avoid serializing out any potential padding.
          WriteValue(column_value_address, block_layout.AttrSize(col_id));
        }
      }
      break;
    }
    case LogRecordType::DELETE: {
      auto *record_body = record.GetUnderlyingRecordBodyAs<DeleteRecord>();
      WriteValue(record_body->GetTableOid());
      WriteValue(record_body->GetTupleSlot());
      break;
    }
    case LogRecordType::COMMIT:
      WriteValue(record.GetUnderlyingRecordBodyAs<CommitRecord>()->CommitTime());
  }
}

void LogManager::WriteValue(const void *val, uint32_t size) {
  // Serialize the value and copy it to the buffer
  BufferedLogWriter *out = GetCurrentWriteBuffer();
  uint32_t size_written = 0;

  while (size_written < size) {
    const byte *val_byte = reinterpret_cast<const byte *>(val) + size_written;
    size_written += out->BufferWrite(val_byte, size - size_written);
    if (out->IsBufferFull()) {
      // Mark the buffer full for the log consumer thread to flush it
      HandFilledBufferToWriter();
      // Get an empty buffer for writing this value
      out = GetCurrentWriteBuffer();
    }
  }
}
}  // namespace terrier::storage
