#include "common/thread_context.h"

#include "metrics/metrics_manager.h"
#include "metrics/metrics_store.h"

namespace terrier::common {

thread_local common::ThreadContext thread_context;

ThreadContext::~ThreadContext() {
  if (metrics_store_ != nullptr) metrics_store_->MetricsManager()->UnregisterThread();
}
}  // namespace terrier::common
