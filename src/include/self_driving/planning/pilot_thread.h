#pragma once

#include <chrono>  //NOLINT
#include <thread>  //NOLINT

#include "self_driving/planning/pilot.h"

namespace noisepage::selfdriving {

/**
 * Class for spinning off a thread that runs the pilot to process query predictions.
 * This should be used in most cases to enable/disable Pilot in the system.
 */
class PilotThread {
 public:
  /**
   * @param pilot Pointer to the pilot object to be run on this thread
   * @param pilot_interval Sleep time between Pilot invocations
   * @param forecaster_train_interval Sleep time between training forecast model
   * @param pilot_planning if the pilot is enabled
   */
  PilotThread(common::ManagedPointer<selfdriving::Pilot> pilot, std::chrono::microseconds pilot_interval,
              std::chrono::microseconds forecaster_train_interval, bool pilot_planning);

  ~PilotThread() { StopPilot(); }

  /**
   * Kill the Pilot thread.
   */
  void StopPilot() {
    NOISEPAGE_ASSERT(run_pilot_, "Pilot should already be running.");
    run_pilot_ = false;
    pilot_paused_ = true;
    pilot_thread_.join();
  }

  /**
   * Spawn the Pilot thread if it has been previously stopped.
   */
  void StartPilot() {
    NOISEPAGE_ASSERT(!run_pilot_, "Pilot should not already be running.");
    run_pilot_ = true;
    pilot_paused_ = true;
    pilot_thread_ = std::thread([this] { PilotThreadLoop(); });
  }

  /**
   * Pause the Pilot from running, typically for use in tests when the state of tables need to be fixed.
   */
  void DisablePilot() {
    NOISEPAGE_ASSERT(!pilot_paused_, "Pilot should not already be paused.");
    pilot_paused_ = true;
  }

  /**
   * Resume Pilot after being paused.
   */
  void EnablePilot() {
    NOISEPAGE_ASSERT(pilot_paused_, "Pilot should already be paused.");
    pilot_paused_ = false;
  }

  /**
   * @return the underlying Pilot object, mostly to register indexes currently.
   */
  common::ManagedPointer<Pilot> GetPilot() { return pilot_; }

 private:
  const common::ManagedPointer<selfdriving::Pilot> pilot_;
  volatile bool run_pilot_;
  volatile bool pilot_paused_;
  std::chrono::microseconds pilot_interval_;
  std::chrono::microseconds forecaster_train_interval_;
  std::chrono::microseconds forecaster_remain_period_;
  std::thread pilot_thread_;

  void PilotThreadLoop() {
    while (run_pilot_) {
      std::this_thread::sleep_for(pilot_interval_);
      forecaster_remain_period_ -=
          ((forecaster_remain_period_ > pilot_interval_) ? pilot_interval_ : forecaster_remain_period_);

      if (!pilot_paused_) {
        if (forecaster_remain_period_ == std::chrono::microseconds::zero()) {
          pilot_->PerformForecasterTrain();
          forecaster_remain_period_ = forecaster_train_interval_;
        } else {
          pilot_->PerformPlanning();
        }
      }
    }
  }
};

}  // namespace noisepage::selfdriving
