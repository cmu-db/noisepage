#include "transaction/deferred_action_thread.h"
#include "transaction/deferred_action_manager.h"

namespace terrier::transaction {
    void DeferredActionThread::DeferredActionThreadLoop() {
    while (run_deferred_events_) {
      std::this_thread::sleep_for(deferred_actions_period_);
      if (!deferred_events_paused_) deferred_actions_manager_->Process();
    }
  }
}