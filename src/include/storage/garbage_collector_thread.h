#pragma once

#include <chrono>  //NOLINT
#include <thread>  //NOLINT
#include "transaction/deferred_action_manager.h"
#include "storage/index/index_gc.h"


// TODO(Tianyu): Should this be in the storage namespace?
namespace terrier::storage {

/**
 * Class for spinning off a thread that runs garbage collection at a fixed interval. This should be used in most cases
 * to enable GC in the system unless you need fine-grained control over table state or profiling.
 */
class GarbageCollectorThread {
 public:
  /**
   * @param txn_manager pointer to the txn manager for the GC to communicate with
   * @param gc_period sleep time between GC invocations
   */
  GarbageCollectorThread(transaction::DeferredActionManager *deferred_action_manager,
                         storage::index::IndexGC *index_gc,
                         const std::chrono::milliseconds gc_period)
      : deferred_action_manager_(deferred_action_manager),
        index_gc_(index_gc),
        run_gc_(true),
        gc_paused_(false),
        gc_period_(gc_period),
        gc_thread_(std::thread([this] { GCThreadLoop(); })) {}

  ~GarbageCollectorThread() {
    run_gc_ = false;
    gc_thread_.join();
    // TODO(Tianyu): how do you cleanly shut down the deferred event framework?
    // This is not 100% correct if there are still live transaction. Maybe this shut down
    // should be put into the destructor of the deferred action manager, such that the
    // transaction manager's dependency on it ensures that the transaction manager is shut down before
    // the deferred action manager.
    while (deferred_action_manager_->Process() != 0);
  }

  /**
   * Pause the GC from running, typically for use in tests when the state of tables need to be fixed.
   */
  void PauseGC() {
    TERRIER_ASSERT(!gc_paused_, "GC should not already be paused.");
    gc_paused_ = true;
  }

  /**
   * Resume GC after being paused.
   */
  void ResumeGC() {
    TERRIER_ASSERT(gc_paused_, "GC should already be paused.");
    gc_paused_ = false;
  }


 private:
  transaction::DeferredActionManager *deferred_action_manager_;
  storage::index::IndexGC *index_gc_;
  volatile bool run_gc_ = true;
  volatile bool gc_paused_ = false;
  std::chrono::milliseconds gc_period_;
  std::thread gc_thread_{[this] { GCThreadLoop(); }};

  void GCThreadLoop() {
    while (run_gc_) {
      std::this_thread::sleep_for(gc_period_);
      if (!gc_paused_) {
        deferred_action_manager_->Process();
        index_gc_->Process();
      }
    }
  }
};

}  // namespace terrier::storage
