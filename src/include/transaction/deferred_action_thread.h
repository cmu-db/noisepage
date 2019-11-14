#pragma once

#include <chrono>  //NOLINT
#include <thread>  //NOLINT
#include "di/di_help.h"
#include "transaction/deferred_action_manager.h"

namespace terrier::transaction {

/**
 * Class for spinning off a thread that runs deferred actions at a fixed interval. This should be used in most cases
 * to enable deferred actions in the system unless you need fine-grained control over table state or profiling.
 */
class DeferredActionThread {
 public:
  DECLARE_ANNOTATION(GC_PERIOD)  // TODO(yash): Change this to deferred events rather than gc
  /**
   * @param deferred_actions_manager pointer to the deferred action manager object to be run on this thread
   * @param deferred_actions_period sleep time between deferred actions process invocations
   */
  DeferredActionThread(transaction::DeferredActionManager *deferred_actions_manager,
                       std::chrono::milliseconds deferred_actions_period)
      : deferred_actions_manager_(deferred_actions_manager),
        run_deferred_events_(true),
        deferred_events_paused_(false),
        deferred_actions_period_(deferred_actions_period),
        deferred_actions_thread_(std::thread([this] { DeferredActionThreadLoop(); })){};

  ~DeferredActionThread() {
    run_deferred_events_ = false;
    deferred_actions_thread_.join();
    // Make sure all garbage is collected. This takes 3 runs for unlink and deallocate, as well as catalog deallocations
    deferred_actions_manager_->Process();
    deferred_actions_manager_->Process();
    deferred_actions_manager_->Process();
  }

  /**
   * Pause the Deferred Actions thread from running, typically for use in tests when the state of tables need to be
   * fixed.
   */
  void PauseDeferredActions() {
    TERRIER_ASSERT(!deferred_events_paused_, "Deferred actions should not already be paused.");
    deferred_events_paused_ = true;
  }

  /**
   * Resume Deferred Actions thread after being paused.
   */
  void ResumeDeferredActions() {
    TERRIER_ASSERT(deferred_events_paused_, "Deferred actions should already be paused.");
    deferred_events_paused_ = false;
  }

  /**
   * @return the underlying DeferredActionsManager object, mostly to register indexes currently.
   */
  DeferredActionManager &GetDeferredActionManager() { return *deferred_actions_manager_; }

 private:
  transaction::DeferredActionManager *deferred_actions_manager_;
  volatile bool run_deferred_events_;
  volatile bool deferred_events_paused_;

  std::chrono::milliseconds deferred_actions_period_;
  std::thread deferred_actions_thread_;

  void DeferredActionThreadLoop();
};

}  // namespace terrier::transaction
