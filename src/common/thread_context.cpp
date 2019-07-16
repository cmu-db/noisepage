#include "common/thread_context.h"
#include "metrics/metrics_manager.h"

namespace terrier::common {

thread_local common::ThreadContext thread_context{nullptr, nullptr};

ThreadContext::~ThreadContext() {
  if (metrics_manager_ != nullptr) metrics_manager_->UnregisterThread();
}
}  // namespace terrier::common
