#include <transaction/transaction_context.h>
#include "storage/write_ahead_log/log_manager.h"

namespace terrier::storage {
void LogManager::Process() {
  while (true) {
    SegmentedBuffer<LogRecord> redo;
    // In a short critical section, try to dequeue an item
    {
      common::SpinLatch::ScopedSpinLatch guard(&flush_queue_latch_);
      if (flush_queue_.empty()) break;
      // This should go out of scope at the end of each loop and destructed
      redo = std::move(flush_queue_.front());
      flush_queue_.pop();
    }
    for (LogRecord &record : redo) {
      if (record.RecordType() == LogRecordType::COMMIT) {
        auto *commit_record = record.GetUnderlyingRecordBodyAs<CommitRecord>();

        // If a transaction is read-only, then the only record it generates is its commit record. This commit record is
        // necessary for the transaction's callback function to be invoked, but there is no need to serialize it, as
        // it corresponds to a transaction with nothing to redo.
        if (!commit_record->IsReadOnly()) {
          SerializeRecord(record);
        }
        commits_in_buffer_.emplace_back(commit_record->Callback(), commit_record->CallbackArg());
        // We are done with this transaction, and we can update the log_head
        TERRIER_ASSERT(transaction::TransactionUtil::NewerThan(commit_record->CommitTime(), log_head_),
                       "transaction's commits should arrive in order");
        log_head_ = commit_record->CommitTime();
      } else {
        // Any record that is not a commit record is always serialized.`
        SerializeRecord(record);
      }
    }
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
    case LogRecordType::COMMIT:WriteValue(record.GetUnderlyingRecordBodyAs<CommitRecord>()->CommitTime());
  }
}

}  // namespace terrier::storage
