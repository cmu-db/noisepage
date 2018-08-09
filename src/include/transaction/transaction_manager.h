#pragma once
#include <tbb/reader_writer_lock.h>
#include "common/spin_latch.h"
#include "common/typedefs.h"
#include "transaction/transaction_context.h"

namespace terrier::transaction {
class TransactionManager {
  // TODO(Tianyu): Implement the global transaction tables
 public:
  TransactionContext BeginTransaction() {
    tbb::reader_writer_lock::scoped_lock_read guard(commit_latch_);
    return {time_++, txn_id_++, object_pool_};
  }

  void Commit(TransactionContext *txn) {
    tbb::reader_writer_lock::scoped_lock guard(commit_latch_);
    timestamp_t commit_time = time_++;
    // Flip all timestamps to be committed
    UndoBuffer undos = txn->GetUndoBuffer();
    for (auto it = undos.Begin(); it != undos.End(); ++it)
      it->timestamp_.store(commit_time);
  }

  void Abort(TransactionContext *txn) {
    // TODO(Tianyu): How do we get to a DataTable here?
  }


 private:
  common::ObjectPool<UndoBufferSegment> *object_pool_;
  // TODO(Tianyu): Timestamp generation needs to be more efficient
  std::atomic<timestamp_t> time_{timestamp_t(0)};
  std::atomic<timestamp_t> txn_id_{timestamp_t(static_cast<uint64_t>(INT64_MIN))}; // start from "negative" value

  // TODO(Tianyu): Maybe don't use tbb?
  // TODO(Tianyu): This is the famed HyPer Latch. We will need to re-evaluate performance later.
  tbb::reader_writer_lock commit_latch_;
};
}
