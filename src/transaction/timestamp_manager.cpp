#include "transaction/timestamp_manager.h"

namespace terrier::transaction {

timestamp_t TimestampManager::OldestTransactionStartTime() {
  common::SpinLatch::ScopedSpinLatch guard(&curr_running_txns_latch_);

  // If we have the oldest start time cached, we return it
  if (oldest_txn_start_time_ != INVALID_TXN_TIMESTAMP) return oldest_txn_start_time_;

  TXN_LOG_INFO("curr_running_txns_ size: " + std::to_string(curr_running_txns_.size()))

  const auto &oldest_txn = std::min_element(curr_running_txns_.cbegin(), curr_running_txns_.cend());
  timestamp_t result;
  if (oldest_txn != curr_running_txns_.end()) {
    result = *oldest_txn;
    // Only cache if there actually is an active txn
    oldest_txn_start_time_ = result;
  } else {
    result = time_.load();
  }
  return result;
}

void TimestampManager::RemoveTransaction(timestamp_t timestamp) {
  common::SpinLatch::ScopedSpinLatch guard(&curr_running_txns_latch_);
  const size_t ret UNUSED_ATTRIBUTE = curr_running_txns_.erase(timestamp);
  TERRIER_ASSERT(ret == 1, "erased timestamp did not exist");

  // If we are removing the cached oldest active txn, we invalidate the cached timestamp
  if (timestamp == oldest_txn_start_time_) oldest_txn_start_time_ = INVALID_TXN_TIMESTAMP;
}

}

