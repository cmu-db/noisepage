#pragma once
#include <unordered_set>
#include "common/strong_typedef.h"
#include "transaction/transaction_defs.h"

namespace terrier::transaction {
/**
 * Generates timestamps!
 */
class TimestampManager {
 public:
  /**
   * @return unique timestamp based on current time, and advances one tick
   */
  timestamp_t CheckoutTimestamp() { return time_++; }

  timestamp_t CurrentTime() const { return time_.load(); }

  timestamp_t BeginTransaction() {
    timestamp_t start_time;
    {
      // There is a three-way race that needs to be prevented.  Specifically, we
      // cannot allow both a transaction to commit and the GC to poll for the
      // oldest running transaction in between this transaction acquiring its
      // begin timestamp and getting inserted into the current running
      // transactions list.  Using the current running transactions latch
      // prevents the GC from polling and stops the race.  This allows us to
      // replace acquiring a shared instance of the commit latch with a
      // read-only spin-latch and move the allocation out of a critical section.
      common::SpinLatch::ScopedSpinLatch running_guard(&curr_running_txns_latch_);
      start_time = time_++;

      const auto ret UNUSED_ATTRIBUTE = curr_running_txns_.emplace(start_time);
      TERRIER_ASSERT(ret.second, "commit start time should be globally unique");
    }  // Release latch on current running transactions
    return start_time;
  }

  void RemoveTransaction(timestamp_t timestamp) {
    common::SpinLatch::ScopedSpinLatch guard(&curr_running_txns_latch_);
    const size_t ret UNUSED_ATTRIBUTE = curr_running_txns_.erase(timestamp);
    TERRIER_ASSERT(ret == 1, "erased timestamp did not exist");
  }

 private:
  // TODO(Tianyu): Timestamp generation needs to be more efficient (batches)
  // TODO(Tianyu): We don't handle timestamp wrap-arounds. I doubt this would be an issue any time soon.
  std::atomic<timestamp_t> time_{timestamp_t(0)};
  // TODO(Matt): consider a different data structure if this becomes a measured bottleneck
  std::unordered_set<timestamp_t> curr_running_txns_;
  common::SpinLatch curr_running_txns_latch_;
};
}  // namespace terrier::transaction
