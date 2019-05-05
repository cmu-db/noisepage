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
    SerializeTaskBuffer(&task_buffer);
    buffer_pool_->Release(buffer);
  }
  // Mark the last buffer that was written to as full
  if (buffer_to_write_ != nullptr) {
    MarkBufferFull();
  }
  Flush();
}

void LogManager::Flush() {
  // Set the flag for the log writer thread to persist the buffers to disk
  do_persist_ = true;
  // Wait for the log writer thread to persist the logs
  while (do_persist_) {
  }
  // Execute the callbacks for the transactions that have been persisted
  for (auto &callback : commits_in_buffer_) callback.first(callback.second);
  commits_in_buffer_.clear();
}

void LogManager::SerializeTaskBuffer(IterableBufferSegment<LogRecord> *const task_buffer) {
  for (LogRecord &record : *task_buffer) {
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
      auto *data_table = record_body->GetDataTable();
      WriteValue(data_table->TableOid());

      // TODO(Justin): Be careful about how tuple slot is interpreted during real recovery. Right now I think we kind of
      //  sidestep the issue with "bookkeeping".
      WriteValue(record_body->GetTupleSlot());

      auto *delta = record_body->Delta();
      // Write out which column ids this redo record is concerned with. On recovery, we can construct the appropriate
      // ProjectedRowInitializer from these ids and their corresponding block layout.
      WriteValue(delta->NumColumns());
      WriteValue(delta->ColumnIds(), static_cast<uint32_t>(sizeof(col_id_t)) * delta->NumColumns());

      // Write out the null bitmap.
      WriteValue(&(delta->Bitmap()), common::RawBitmap::SizeInBytes(delta->NumColumns()));

      // We need the block layout to determine the size of each attribute.
      const auto &block_layout = data_table->GetBlockLayout();
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
      auto *data_table = record_body->GetDataTable();
      WriteValue(data_table->TableOid());
      WriteValue(record_body->GetTupleSlot());
      break;
    }
    case LogRecordType::COMMIT:
      WriteValue(record.GetUnderlyingRecordBodyAs<CommitRecord>()->CommitTime());
  }
}

void LogManager::WriteToDiskLoop() {
  // Log writer thread spins in this loop
  // It dequeues a filled buffer and flushes it to disk
  while (run_log_writer_thread_) {
    BufferedLogWriter *buf;
    bool dequeued = NonblockingDequeueBuffer(&buf, &filled_buffer_queue_);
    // If the main logger thread has signaled to persist the buffers, persist all the filled buffers
    if (do_persist_) {
      FlushAllBuffers();
      // Signal the main logger thread for completion of persistence
      do_persist_ = false;
    }
    if (!dequeued) {
      continue;
    }
    // Flush the buffer to the disk
    buf->FlushBuffer();
    // Push the emptied buffer to queue of available buffers to fill
    BlockingEnqueueBuffer(buf, &empty_buffer_queue_);
  }
}

void LogManager::FlushAllBuffers() {
  // Persist all the filled buffers to the disk
  bool dequeued;
  do {
    // Dequeue filled buffers and flush them to disk
    BufferedLogWriter *buf;
    dequeued = NonblockingDequeueBuffer(&buf, &filled_buffer_queue_);
    if (dequeued) {
      buf->FlushBuffer();
      // Enqueue the flushed buffer to the empty buffer queue
      BlockingEnqueueBuffer(buf, &empty_buffer_queue_);
    }
  } while (dequeued);
  // Persist the buffers
  TERRIER_ASSERT(!buffers_.empty(), "Buffers vector should not be empty until Shutdown");
  buffers_.front().Persist();
}

void LogManager::WriteValue(const void *val, uint32_t size) {
  // Serialize the value and copy it to the buffer
  BufferedLogWriter *out = GetBufferToWrite();
  uint32_t size_written = 0;

  while (size_written < size) {
    const byte *val_byte = reinterpret_cast<const byte *>(val) + size_written;
    size_written += out->BufferWrite(val_byte, size - size_written);
    if (out->IsBufferFull()) {
      // Mark the buffer full for the log writer thread to flush it
      MarkBufferFull();
      // Get an empty buffer for writing this value
      out = GetBufferToWrite();
    }
  }
}
}  // namespace terrier::storage
