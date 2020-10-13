#include "storage/garbage_collector_thread.h"

#include "metrics/metrics_manager.h"

namespace terrier::storage {
GarbageCollectorThread::GarbageCollectorThread(common::ManagedPointer<GarbageCollector> gc,
                                               std::chrono::microseconds gc_period,
                                               common::ManagedPointer<metrics::MetricsManager> metrics_manager)
    : gc_(gc),
      metrics_manager_(metrics_manager),
      run_gc_(true),
      gc_paused_(false),
      gc_period_(gc_period),
      gc_thread_(std::thread([this] {
        if (metrics_manager_ != DISABLED) metrics_manager_->RegisterThread();
        gc_->SetGCInterval(gc_period_.count());
        GCThreadLoop();
      })) {}

}  // namespace terrier::storage
