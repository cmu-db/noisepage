#include "storage/write_ahead_log/log_manager.h"

namespace terrier::storage {
void LogManager::Process() {
  while (true) {
    common::SpinLatch::ScopedSpinLatch guard(&log_manager_latch_);
    if (flush_queue_.empty()) return;
    RecordBufferSegment *buffer = flush_queue_.front();
    for (LogRecord &record : IterableBufferSegment<LogRecord>(buffer)) {
      SerializeRecord(record);
      if (record.RecordType() == LogRecordType::COMMIT) commits_in_buffer_.push_back(record.TxnBegin());
    }
    buffer_pool_->Release(buffer);
    flush_queue_.pop();
  }
}

void LogManager::Flush() {
  out_.Flush();
  common::SpinLatch::ScopedSpinLatch guard(&log_manager_latch_);
  for (timestamp_t txn : commits_in_buffer_) {
    auto it = callbacks_.find(txn);
    TERRIER_ASSERT(it != callbacks_.end(), "committing transaction does not have a registered callback for flush");
    it->second();
    callbacks_.erase(it);
  }
  commits_in_buffer_.clear();
}

void LogManager::SerializeRecord(const terrier::storage::LogRecord &record) {
  WriteValue(record.Size());
  WriteValue(record.RecordType());
  WriteValue(record.TxnBegin());
  switch (record.RecordType()) {
    case LogRecordType::REDO: {
      auto *record_body = record.GetUnderlyingRecordBodyAs<RedoRecord>();
      WriteValue(record_body->GetDataTable()->TableOid());
      WriteValue(record_body->GetTupleSlot());
      // TODO(Tianyu): Need to inline varlen or other things, and figure out a better representation.
      Write(record_body->Delta(), record_body->Delta()->Size());
      break;
    }
    case LogRecordType::DELETE: {
      auto *record_body = record.GetUnderlyingRecordBodyAs<DeleteRecord>();
      WriteValue(record_body->GetDataTable()->TableOid());
      WriteValue(record_body->GetTupleSlot());
      break;
    }
    case LogRecordType::COMMIT:
      WriteValue(record.GetUnderlyingRecordBodyAs<CommitRecord>()->CommitTime());

  }
}

void LogManager::Write(const void *data, uint32_t size) {
  if (!out_.CanBuffer(size)) Flush();
  if (!out_.CanBuffer(size)) {
    // This write is too large to fit into a buffer, we need to write directly without a buffer,
    // but no flush is necessary since the commit records are always small enough to be buffered
    out_.WriteUnsynced(data, size);
    return;
  }
  // Write can be buffered
  out_.BufferWrite(data, size);
}

}  // namespace terrier::storage
