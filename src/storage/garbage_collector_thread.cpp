#include "storage/garbage_collector_thread.h"

#include "metrics/metrics_manager.h"

namespace noisepage::storage {
GarbageCollectorThread::GarbageCollectorThread(common::ManagedPointer<GarbageCollector> gc,
                                               std::chrono::microseconds gc_period,
                                               common::ManagedPointer<metrics::MetricsManager> metrics_manager,
                                               uint32_t num_daf_threads)
    : gc_(gc),
      metrics_manager_(metrics_manager),
      run_gc_(true),
      gc_paused_(false),
      gc_period_(gc_period),
      num_gc_threads_(num_daf_threads) {
  for (size_t i = 0; i < num_gc_threads_; i++) {
    gc_threads_.emplace_back(std::thread([this] {
      if (metrics_manager_ != DISABLED) metrics_manager_->RegisterThread();
      GCThreadLoop();
    }));
  }
}

}  // namespace noisepage::storage
