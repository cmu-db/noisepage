#pragma once

#include <chrono>  //NOLINT
#include <thread>  //NOLINT

#include "brain/pilot/pilot.h"


namespace terrier::brain {

/**
 * Class for spinning off a thread that runs garbage collection at a fixed interval. This should be used in most cases
 * to enable PL in the system unless you need fine-grained control over table state or profiling.
 */
class PilotThread {
 public:
  /**
   * @param pl pointer to the garbage collector object to be run on this thread
   * @param pl_period sleep time between PL invocations
   */
  PilotThread(common::ManagedPointer<Pilot> pl, std::chrono::microseconds pl_period);

  ~PilotThread() { StopPL(); }

  /**
   * Kill the PL thread and run PL a few times to clean up the system.
   */
  void StopPL() {
    TERRIER_ASSERT(run_pl_, "PL should already be running.");
    run_pl_ = false;
    pl_thread_.join();
    for (uint8_t i = 0; i < transaction::MIN_PL_INVOCATIONS; i++) {
      pl_->PerformPilotLogic();
    }
  }

  /**
   * Spawn the PL thread if it has been previously stopped.
   */
  void StartPL() {
    TERRIER_ASSERT(!run_pl_, "PL should not already be running.");
    run_pl_ = true;
    pl_paused_ = false;
    pl_thread_ = std::thread([this] { PLThreadLoop(); });
  }

  /**
   * Pause the PL from running, typically for use in tests when the state of tables need to be fixed.
   */
  void PausePL() {
    TERRIER_ASSERT(!pl_paused_, "PL should not already be paused.");
    pl_paused_ = true;
  }

  /**
   * Resume PL after being paused.
   */
  void ResumePL() {
    TERRIER_ASSERT(pl_paused_, "PL should already be paused.");
    pl_paused_ = false;
  }

  /**
   * @return the underlying PL object, mostly to register indexes currently.
   */
  common::ManagedPointer<Pilot> GetPilot() { return pl_; }

 private:
  const common::ManagedPointer<brain::pilot::Pilot> pl_;
  volatile bool run_pl_;
  volatile bool pl_paused_;
  std::chrono::microseconds pl_period_;
  std::thread pl_thread_;

  void PLThreadLoop() {
    while (run_pl_) {
      std::this_thread::sleep_for(pl_period_);
      if (!pl_paused_) pl_->PerformPilotLogic();
    }
  }
};

}  // namespace terrier::storage
