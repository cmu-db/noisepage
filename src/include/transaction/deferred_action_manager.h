#pragma once
#include <queue>
#include <utility>
#include <vector>
#include "transaction/timestamp_manager.h"
#include "transaction/transaction_defs.h"

namespace terrier::transaction {
class DeferredActionManager {
 public:
  explicit DeferredActionManager(TimestampManager *timestamp_manager) : timestamp_manager_(timestamp_manager) {}

  timestamp_t RegisterDeferredAction(const DeferredAction &a) {
    timestamp_t result = timestamp_manager_->CurrentTime();
    {
      common::SpinLatch::ScopedSpinLatch guard(&deferred_actions_latch_);
      new_deferred_actions_.emplace(result, a);
    }
    return result;
  }

  uint32_t Process() {
    uint32_t processed = 0;
    // Check out a timestamp from the transaction manager to determine the progress of
    // running transactions in the system.
    timestamp_t oldest_txn = timestamp_manager_->CheckOutTimestamp();
    processed += ClearBacklog(oldest_txn);
    processed += ProcessNewActions(oldest_txn);
    return processed;
  }

 private:
  TimestampManager *timestamp_manager_;
  // TODO(Tianyu): We might want to change this data structure to be more specialized than std::queue
  // Both queue should stay sorted in timestamp
  std::queue<std::pair<timestamp_t, DeferredAction>> new_deferred_actions_, back_log_;
  common::SpinLatch deferred_actions_latch_;


  uint32_t ClearBacklog(timestamp_t oldest_txn) {
    uint32_t processed = 0;
    // Execute as many deferred actions as we can at this time from the backlog.
    // Stop traversing
    while (!back_log_.empty() && back_log_.front().first <= oldest_txn) {
      back_log_.front().second(oldest_txn);
      processed++;
      back_log_.pop();
    }
    return processed;
  }

  uint32_t ProcessNewActions(timestamp_t oldest_txn) {
    uint32_t processed = 0;
    // swap the new actions queue with a local queue, so the rest of the system can continue
    // while we process actions
    std::queue<std::pair<timestamp_t, DeferredAction>> new_actions_local;
    {
      common::SpinLatch::ScopedSpinLatch guard(&deferred_actions_latch_);
      new_actions_local = std::move(new_deferred_actions_);
    }

    // Iterate through the new actions queue and execute as many as possible
    while (!new_actions_local.empty() && new_actions_local.front().first <= oldest_txn) {
      new_actions_local.front().second(oldest_txn);
      processed++;
      new_actions_local.pop();
    }

    // Add the rest to back log otherwise
    while (!new_actions_local.empty()) {
      back_log_.push(new_actions_local.front());
      new_actions_local.pop();
    }
    return processed;
  }
};
}  // namespace terrier::transaction
