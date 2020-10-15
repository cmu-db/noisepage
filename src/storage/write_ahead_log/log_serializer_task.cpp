#include "storage/write_ahead_log/log_serializer_task.h"

#include <queue>
#include <utility>

#include "common/scoped_timer.h"
#include "common/thread_context.h"
#include "metrics/metrics_store.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"

namespace terrier::storage {

void LogSerializerTask::LogSerializerTaskLoop() {
  auto curr_sleep = serialization_interval_;
  // TODO(Gus): Make max back-off a settings manager setting
  const std::chrono::microseconds max_sleep =
      std::chrono::microseconds(10000);  // We cap the back-off in case of long gaps with no transactions

  uint64_t num_bytes = 0, num_records = 0, num_txns = 0;

  do {
    const bool logging_metrics_enabled =
        common::thread_context.metrics_store_ != nullptr &&
        common::thread_context.metrics_store_->ComponentToRecord(metrics::MetricsComponent::LOGGING);

    if (logging_metrics_enabled && !common::thread_context.resource_tracker_.IsRunning()) {
      // start the operating unit resource tracker
      common::thread_context.resource_tracker_.Start();
    }

    // Serializing is now on the "critical txn path" because txns wait to commit until their logs are serialized. Thus,
    // a sleep is not fast enough. We perform exponential back-off, doubling the sleep duration if we don't process any
    // buffers in our call to Process. Calls to Process will process as long as new buffers are available. We only
    // sleep as part of this exponential backoff when there are logs that need to be processed and we wake up when there
    // are new logs to be processed.
    if (empty_) {
      std::unique_lock<std::mutex> guard(flush_queue_latch_);
      sleeping_ = true;
      flush_queue_cv_.wait_for(guard, curr_sleep);
      sleeping_ = false;
    }

    // If Process did not find any new buffers, we perform exponential back-off to reduce our rate of polling for new
    // buffers. We cap the maximum back-off, since in the case of large gaps of no txns, we don't want to unboundedly
    // sleep
    std::tie(num_bytes, num_records, num_txns) = Process();
    curr_sleep = std::min(num_records > 0 ? serialization_interval_ : curr_sleep * 2, max_sleep);

    if (logging_metrics_enabled && num_records > 0) {
      // Stop the resource tracker for this operating unit
      common::thread_context.resource_tracker_.Stop();
      auto &resource_metrics = common::thread_context.resource_tracker_.GetMetrics();
      common::thread_context.metrics_store_->RecordSerializerData(num_bytes, num_records, num_txns,
                                                                  serialization_interval_.count(), resource_metrics);
      num_bytes = num_records = num_txns = 0;
    }
  } while (run_task_);
  // To be extra sure we processed everything
  Process();
  TERRIER_ASSERT(flush_queue_.empty(), "Termination of LogSerializerTask should hand off all buffers to consumers");
}

std::tuple<uint64_t, uint64_t, uint64_t> LogSerializerTask::Process() {
  uint64_t num_bytes = 0, num_records = 0, num_txns = 0;

  bool buffers_processed = false;

  {
    common::SpinLatch::ScopedSpinLatch serialization_guard(&serialization_latch_);
    TERRIER_ASSERT(serialized_txns_.empty(),
                   "Aggregated txn timestamps should have been handed off to TimestampManager");
    // We continually grab all the buffers until we find there are no new buffers. This way we serialize buffers that
    // came in during the previous serialization loop

    // Continually loop, break out if there's no new buffers
    while (true) {
      // In a short critical section, get all buffers to serialize. We move them to a temp queue to reduce contention on
      // the queue transactions interact with
      {
        std::unique_lock<std::mutex> guard(flush_queue_latch_);

        // There are no new buffers, so we can break
        if (flush_queue_.empty()) {
          break;
        }

        temp_flush_queue_ = std::move(flush_queue_);
        flush_queue_ = std::queue<RecordBufferSegment *>();
        empty_ = true;
      }

      // Loop over all the new buffers we found
      while (!temp_flush_queue_.empty()) {
        RecordBufferSegment *buffer = temp_flush_queue_.front();
        temp_flush_queue_.pop();

        // Serialize the Redo buffer and release it to the buffer pool
        IterableBufferSegment<LogRecord> task_buffer(buffer);
        const auto num_bytes_records_and_txns = SerializeBuffer(&task_buffer);
        buffer_pool_->Release(buffer);
        num_bytes += std::get<0>(num_bytes_records_and_txns);
        num_records += std::get<1>(num_bytes_records_and_txns);
        num_txns += std::get<2>(num_bytes_records_and_txns);
      }

      buffers_processed = true;
    }

    // Mark the last buffer that was written to as full
    if (buffers_processed) HandFilledBufferToWriter();

    // Bulk remove all the transactions we serialized. This prevents having to take the TimestampManager's latch once
    // for each timestamp we remove.
    for (const auto &txns : serialized_txns_) {
      txns.first->RemoveTransactions(txns.second);
    }
    serialized_txns_.clear();
  }

  return {num_bytes, num_records, num_txns};
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

std::tuple<uint64_t, uint64_t, uint64_t> LogSerializerTask::SerializeBuffer(
    IterableBufferSegment<LogRecord> *buffer_to_serialize) {
  uint64_t num_bytes = 0, num_records = 0, num_txns = 0;

  // Iterate over all redo records in the redo buffer through the provided iterator
  for (LogRecord &record : *buffer_to_serialize) {
    switch (record.RecordType()) {
      case (LogRecordType::COMMIT): {
        auto *commit_record = record.GetUnderlyingRecordBodyAs<CommitRecord>();

        // If a transaction is read-only, then the only record it generates is its commit record. This commit record is
        // necessary for the transaction's callback function to be invoked, but there is no need to serialize it, as
        // it corresponds to a transaction with nothing to redo.
        if (!commit_record->IsReadOnly()) num_bytes += SerializeRecord(record);
        commits_in_buffer_.emplace_back(commit_record->CommitCallback(), commit_record->CommitCallbackArg());
        // Once serialization is done, we notify the txn manager to let GC know this txn is ready to clean up
        serialized_txns_[commit_record->TimestampManager()].push_back(record.TxnBegin());
        num_txns++;
        break;
      }

      case (LogRecordType::ABORT): {
        // If an abort record shows up at all, the transaction cannot be read-only
        num_bytes += SerializeRecord(record);
        auto *abord_record = record.GetUnderlyingRecordBodyAs<AbortRecord>();
        serialized_txns_[abord_record->TimestampManager()].push_back(record.TxnBegin());
        num_txns++;
        break;
      }

      default:
        // Any record that is not a commit record is always serialized.`
        num_bytes += SerializeRecord(record);
    }
    num_records++;
  }

  return {num_bytes, num_records, num_txns};
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

      // Write out the attr sizes boundaries, this way we can deserialize the records without the need of the block
      // layout
      const auto &block_layout = record_body->GetTupleSlot().GetBlock()->data_table_->GetBlockLayout();
      uint16_t boundaries[NUM_ATTR_BOUNDARIES];
      memset(boundaries, 0, sizeof(uint16_t) * NUM_ATTR_BOUNDARIES);
      StorageUtil::ComputeAttributeSizeBoundaries(block_layout, delta->ColumnIds(), delta->NumColumns(), boundaries);
      WriteValue(boundaries, sizeof(uint16_t) * NUM_ATTR_BOUNDARIES);

      // Write out the null bitmap.
      num_bytes += WriteValue(&(delta->Bitmap()), common::RawBitmap::SizeInBytes(delta->NumColumns()));

      // Write out attribute values
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
      auto *record_body = record.GetUnderlyingRecordBodyAs<CommitRecord>();
      num_bytes += WriteValue(record_body->CommitTime());
      num_bytes += WriteValue(record_body->OldestActiveTxn());
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
