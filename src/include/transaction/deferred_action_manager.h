#pragma once
#include <queue>
#include <utility>
#include <vector>
#include "common/shared_latch.h"
#include "storage/index/index.h"
#include "transaction/timestamp_manager.h"
#include "transaction/transaction_defs.h"

namespace terrier{
  class TransactionTestUtil;
}
namespace terrier::transaction {
  class DeferredActionThread;
/**
 * The deferred action manager tracks deferred actions and provides a function to process them
 */
class DeferredActionManager {
 public:
  /**
   * Constructs a new DeferredActionManager
   * @param timestamp_manager source of timestamps in the system
   */
  DeferredActionManager(TimestampManager *timestamp_manager)  // NOLINT
  : timestamp_manager_(timestamp_manager){};

  ~DeferredActionManager() {
    common::SpinLatch::ScopedSpinLatch guard(&deferred_actions_latch_);
    TERRIER_ASSERT(back_log_.empty(), "Backlog is not empty");
    TERRIER_ASSERT(new_deferred_actions_.empty(), "Some deferred actions remaining at time of destruction");
  }

  /**
   * Adds the action to a buffered list of deferred actions.  This action will
   * be triggered no sooner than when the epoch (timestamp of oldest running
   * transaction) is more recent than the time this function was called.
   * @param a functional implementation of the action that is deferred. @see DeferredAction
   */
  timestamp_t RegisterDeferredAction(const DeferredAction &a);

  /**
   * Adds the action to a buffered list of deferred actions.  This action will
   * be triggered no sooner than when the epoch (timestamp of oldest running
   * transaction) is more recent than the time this function was called.
   * @param a functional implementation of the action that is deferred
   */
  timestamp_t RegisterDeferredAction(const std::function<void()> &a);

  /**
   * Clear the queue and apply as many actions as possible
   * @return numbers of deferred actions processed
   */
  uint32_t Process();

  /**
   * TOOD(yash): Clean this up when removing bw-tree
   * Register an index to be periodically garbage collected
   * @param index pointer to the index to register
   */
  void RegisterIndexForGC(common::ManagedPointer<storage::index::Index> index);

  /**
   * TOOD(yash): Clean this up when removing bw-tree
   * Unregister an index to be periodically garbage collected
   * @param index pointer to the index to unregister
   */
  void UnregisterIndexForGC(common::ManagedPointer<storage::index::Index> index);

 private:
  friend class transaction::DeferredActionThread; 
  friend class terrier::TransactionTestUtil;

  TimestampManager *timestamp_manager_;
  // TODO(Tianyu): We might want to change this data structure to be more specialized than std::queue
  std::queue<std::pair<timestamp_t, DeferredAction>> new_deferred_actions_, back_log_;
  common::SpinLatch deferred_actions_latch_;

  std::unordered_set<common::ManagedPointer<storage::index::Index>> indexes_;
  common::SharedLatch indexes_latch_;

  uint32_t ClearBacklog(timestamp_t oldest_txn);

  uint32_t ProcessNewActions(timestamp_t oldest_txn);

  void ProcessIndexes();
};
}  // namespace terrier::transaction
