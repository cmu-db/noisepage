#pragma once

#include <algorithm>
#include <unordered_set>
#include <vector>

#include "common/spin_latch.h"
#include "common/strong_typedef.h"
#include "transaction/transaction_defs.h"

namespace noisepage::storage {
// Forward declaration
class LogSerializerTask;
}  // namespace noisepage::storage

namespace noisepage::transaction {
class TransactionManager;
/**
 * Generates timestamps, and keeps track of the lifetime of transactions (whether they have entered or left the system)
 */
class TimestampManager {
 public:
  ~TimestampManager() {
    NOISEPAGE_ASSERT(curr_running_txns_.empty(),
                     "Destroying the TimestampManager while txns are still running. That seems wrong.");
  }

  /**
   * @return unique timestamp based on current time, and advances one tick
   */
  timestamp_t CheckOutTimestamp() { return time_++; }

  /**
   * @return current time without advancing the tick
   */
  timestamp_t CurrentTime() const { return time_.load(); }

  /**
   * Get the oldest transaction alive (by start timestamp given out by this timestamp manager at this time)
   * Because of concurrent operations, it is not guaranteed that upon return the txn is still alive. However,
   * it is guaranteed that the return timestamp is older than any transactions live.
   * @warning If logging is enabled, txns are not removed from the txn set until they are serialized. Thus, the active
   * txn set can grow greatly in size, making this call expensive. Consider using CachedOldestTransactionStartTime for
   * better peformance at the cost of a more stale timestamp.
   * @return timestamp that is older than any transactions alive
   */
  timestamp_t OldestTransactionStartTime();

  /**
   * Get the cached timestamp of the oldest active txn. The cached timestamp is only refreshed upon every invocation of
   * OldestTransactionStartTime, so it may be stale. On the other hand, this function does not require taking a latch or
   * iterating through the running txns set, making it much cheaper than OldestTransactionStartTime. This has the same
   * correctness guarantee as OldestTransactionStartTime, but may cause performance degradations for processes that rely
   * on very fresh oldest txn timestamps
   * @return timestamp that is older than any transactions alive
   */
  timestamp_t CachedOldestTransactionStartTime();

 private:
  // TransactionManager needs to be able to use the curr_running_txns_latch to guard more than just the
  // duration of the following method calls --- things such as adding to the GC queue needs to be atomic
  // along with removing a transaction from the table of active transactions. We need this for correctness
  // in the deferred action framework when dropping tables.
  friend class TransactionManager;
  friend class storage::LogSerializerTask;
  timestamp_t BeginTransaction() {
    timestamp_t start_time;
    {
      common::SpinLatch::ScopedSpinLatch running_guard(&curr_running_txns_latch_);
      // There is a three-way race that needs to be prevented.  Specifically, we
      // cannot allow both a transaction to commit and the GC to poll for the
      // oldest running transaction in between this transaction acquiring its
      // begin timestamp and getting inserted into the current running
      // transactions list.  Using the current running transactions latch
      // prevents the GC from polling and stops the race.  This allows us to
      // replace acquiring a shared instance of the commit latch with a
      // read-only spin-latch and move the allocation out of a critical section.
      start_time = time_++;

      const auto ret UNUSED_ATTRIBUTE = curr_running_txns_.emplace(start_time);
      NOISEPAGE_ASSERT(ret.second, "commit start time should be globally unique");
    }  // Release latch on current running transactions
    return start_time;
  }

  /**
   * Remove a timestamp from active txn set
   * @param timestamp timestamp to remove
   */
  void RemoveTransaction(timestamp_t timestamp);

  /**
   * Bulk remove a set of timestamps from the active txn set. Only grabs the curr_running_txns_latch_ once for all the
   * timestamps.
   * @param timestamps vector of timestamps to remove
   */
  void RemoveTransactions(const std::vector<timestamp_t> &timestamps);

  // TODO(Tianyu): Timestamp generation needs to be more efficient (batches)
  // TODO(Tianyu): We don't handle timestamp wrap-arounds. I doubt this would be an issue any time soon.
  std::atomic<timestamp_t> time_{INITIAL_TXN_TIMESTAMP};
  // We cache the oldest txn start time
  std::atomic<timestamp_t> cached_oldest_txn_start_time_{INITIAL_TXN_TIMESTAMP};
  // TODO(Matt): consider a different data structure if this becomes a measured bottleneck
  // TODO(Gus): This data structure initially only held items in the order of # of workers. With the logging change, it
  // can hold many more, since txns are only removed when serialized. We should consider if there is a possible better
  // data structure
  std::unordered_set<timestamp_t> curr_running_txns_;
  mutable common::SpinLatch curr_running_txns_latch_;
};
}  // namespace noisepage::transaction
