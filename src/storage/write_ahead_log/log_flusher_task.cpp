#include "storage/write_ahead_log/log_flusher_task.h"

namespace terrier::storage {

void LogFlusherTask::LogFlusherTaskLoop() {
  do {
    std::this_thread::sleep_for(flushing_interval_);
    log_manager_->ForceFlush();
  } while (run_task_);
}

}  // namespace terrier::storage
