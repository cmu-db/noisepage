#include "transaction/timestamp_manager.h"

#include <algorithm>
#include <vector>

namespace noisepage::transaction {

timestamp_t TimestampManager::OldestTransactionStartTime() {
  common::SpinLatch::ScopedSpinLatch guard(&curr_running_txns_latch_);
  const auto &oldest_txn = std::min_element(curr_running_txns_.cbegin(), curr_running_txns_.cend());
  timestamp_t result = (oldest_txn != curr_running_txns_.end()) ? *oldest_txn : time_.load();
  cached_oldest_txn_start_time_.store(result);  // Cache the timestamp
  return result;
}

timestamp_t TimestampManager::CachedOldestTransactionStartTime() { return cached_oldest_txn_start_time_.load(); }

void TimestampManager::RemoveTransaction(timestamp_t timestamp) {
  common::SpinLatch::ScopedSpinLatch guard(&curr_running_txns_latch_);
  const size_t ret UNUSED_ATTRIBUTE = curr_running_txns_.erase(timestamp);
  NOISEPAGE_ASSERT(ret == 1, "erased timestamp did not exist");
}

void TimestampManager::RemoveTransactions(const std::vector<noisepage::transaction::timestamp_t> &timestamps) {
  common::SpinLatch::ScopedSpinLatch guard(&curr_running_txns_latch_);
  for (const auto &timestamp : timestamps) {
    const size_t ret UNUSED_ATTRIBUTE = curr_running_txns_.erase(timestamp);
    NOISEPAGE_ASSERT(ret == 1, "erased timestamp did not exist");
  }
}

}  // namespace noisepage::transaction
