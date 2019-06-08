#pragma once

#include <chrono>  // NOLINT
#include <thread>  // NOLINT
#include "transaction/deferred_action_manager.h"

namespace terrier::transaction {
class DeferredActionThread {
 public:
  DeferredActionThread(DeferredActionManager *manager, const std::chrono::milliseconds run_period)
    : manager_(manager), run_period_(run_period) {}

  ~DeferredActionThread() {
    run_ = false;
    thread_.join();

  }

  void Pause() {
    TERRIER_ASSERT(!paused_, "The thread should not already be paused");
    paused_ = true;
  }

  void Resume() {
    TERRIER_ASSERT(paused_, "The thread should already be paused");
    paused_ = false;
  }

 private:
  DeferredActionManager *manager_;
  volatile bool run_ = true;
  volatile bool paused_ = false;
  std::chrono::milliseconds run_period_;
  std::thread thread_{[this] { ThreadLoop(); }};

  void ThreadLoop() {
    while (run_) {
      std::this_thread::sleep_for(run_period_);
      if (!paused_) manager_->Process();
    }
  }
};
}
