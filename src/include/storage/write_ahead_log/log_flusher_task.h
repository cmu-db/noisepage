#pragma once

#include "storage/write_ahead_log/log_manager.h"

namespace terrier::storage {

// Forward declaration of LogManager
class LogManager;

/**
 * Task signals disk log consumer to flush logs into disk
 */
class LogFlusherTask : public DedicatedThreadTask {
 public:
  /**
   * @param log_manager Pointer to log manager
   * @param flushing_interval interval time for when to trigger flushing
   */
  explicit LogFlusherTask(LogManager *log_manager, const std::chrono::milliseconds flushing_interval)
      : log_manager_(log_manager), flushing_interval_(flushing_interval), run_task_(false) {}

  /**
   * Runs main disk log writer loop. Called by thread registry upon initialization of thread
   */
  void RunTask() override {
    run_task_ = true;
    LogFlusherTaskLoop();
  }

  /**
   * Signals task to stop. Called by thread registry upon termination of thread
   */
  void Terminate() override {
    // If the task hasn't run yet, sleep
    while (!run_task_) std::this_thread::sleep_for(flushing_interval_);
    TERRIER_ASSERT(run_task_, "Cant terminate a task that isnt running");
    run_task_ = false;
  }

 private:
  LogManager *log_manager_;
  const std::chrono::milliseconds flushing_interval_;
  bool run_task_;

  /**
   * Main log flush loop. Calls ForceFlush on LogManager every interval
   */
  void LogFlusherTaskLoop();
};
}  // namespace terrier::storage
