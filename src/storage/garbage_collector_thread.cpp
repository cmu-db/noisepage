#include "storage/garbage_collector_thread.h"
#include "metrics/metrics_manager.h"

namespace terrier::storage {
GarbageCollectorThread::GarbageCollectorThread(common::ManagedPointer<GarbageCollector> gc,
                                               std::chrono::milliseconds gc_period,
                                               common::ManagedPointer<metrics::MetricsManager> metrics_manager, common::ManagedPointer<storage::LogManager> log_manager)
    : gc_(gc),
      metrics_manager_(metrics_manager),
      log_manager_(log_manager),
      run_gc_(true),
      gc_paused_(false),
      gc_period_(gc_period) {
  for (size_t i = 0; i < NUM_GC_THREADS; i++) {
    gc_threads_.emplace_back(std::thread([this, i] {
      if (metrics_manager_ != DISABLED) metrics_manager_->RegisterThread();
      GCThreadLoop(i == 0);
    }));
  }
}

}  // namespace terrier::storage
