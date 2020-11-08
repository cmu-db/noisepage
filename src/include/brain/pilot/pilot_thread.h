#pragma once

#include <chrono>  //NOLINT
#include <thread>  //NOLINT

#include "brain/pilot/pilot.h"

namespace noisepage::brain {

/**
 * Class for spinning off a thread that runs garbage collection at a fixed interval. This should be used in most cases
 * to enable PL in the system unless you need fine-grained control over table state or profiling.
 */
class PilotThread {
 public:
  /**
   * @param pl pointer to the garbage collector object to be run on this thread
   * @param pl_period sleep time between PL invocations
   * @param metrics_manager Metrics Manager
   */
  PilotThread(common::ManagedPointer<Pilot> pilot, std::chrono::microseconds pl_period, bool pilot_planning);

  ~PilotThread() { StopPL(); }

  /**
   * Kill the PL thread.
   */
  void StopPL() {
    NOISEPAGE_ASSERT(run_pl_, "PL should already be running.");
    run_pl_ = false;
    pl_paused_ = true;
    pl_thread_.join();
  }

  /**
   * Spawn the PL thread if it has been previously stopped.
   */
  void StartPL() {
    NOISEPAGE_ASSERT(!run_pl_, "PL should not already be running.");
    run_pl_ = true;
    pl_paused_ = true;
    pl_thread_ = std::thread([this] { PLThreadLoop(); });
  }

  /**
   * Pause the PL from running, typically for use in tests when the state of tables need to be fixed.
   */
  void DisablePL() {
    NOISEPAGE_ASSERT(!pl_paused_, "PL should not already be paused.");
    pl_paused_ = true;
  }

  /**
   * Resume PL after being paused.
   */
  void EnablePL() {
    NOISEPAGE_ASSERT(pl_paused_, "PL should already be paused.");
    pl_paused_ = false;
  }

  /**
   * @return the underlying PL object, mostly to register indexes currently.
   */
  common::ManagedPointer<Pilot> GetPilot() { return pl_; }

 private:
  const common::ManagedPointer<brain::Pilot> pl_;
  volatile bool run_pl_;
  volatile bool pl_paused_;
  std::chrono::microseconds pl_period_;
  std::thread pl_thread_;

  void PLThreadLoop() {
    while (run_pl_) {
      std::this_thread::sleep_for(pl_period_);
      if (!pl_paused_) pl_->PerformPlanning();
    }
  }
};

}  // namespace noisepage::brain
