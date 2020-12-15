#include "loggers/metrics_logger.h"

namespace noisepage::metrics {
#ifdef NOISEPAGE_USE_LOGGING
common::SanctionedSharedPtr<spdlog::logger>::Ptr metrics_logger = nullptr;

void InitMetricsLogger() {
  if (metrics_logger == nullptr) {
    metrics_logger = std::make_shared<spdlog::logger>("metrics_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(metrics_logger);
  }
}
#endif
}  // namespace noisepage::metrics
