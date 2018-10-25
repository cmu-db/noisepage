#include "storage/write_ahead_log/log_manager.h"

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
    for (LogRecord &record : IterableBufferSegment<LogRecord>(buffer)) {
      SerializeRecord(record);
      if (record.RecordType() == LogRecordType::COMMIT) {
        auto *commit_record = record.GetUnderlyingRecordBodyAs<CommitRecord>();
        commits_in_buffer_.emplace_back(commit_record->Callback(), commit_record->CallbackArg());
      }
    }
    buffer_pool_->Release(buffer);
  }
  Flush();
}

void LogManager::Flush() {
  out_.Persist();
  for (auto &callback : commits_in_buffer_) callback.first(callback.second);
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
      out_.BufferWrite(record_body->Delta(), record_body->Delta()->Size());
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

}  // namespace terrier::storage
