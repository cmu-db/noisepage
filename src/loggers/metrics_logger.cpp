#include "loggers/metrics_logger.h"

#include <memory>

namespace noisepage::metrics {
#ifdef NOISEPAGE_USE_LOGGING
std::shared_ptr<spdlog::logger> metrics_logger = nullptr;  // NOLINT

void InitMetricsLogger() {
  if (metrics_logger == nullptr) {
    metrics_logger = std::make_shared<spdlog::logger>("metrics_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(metrics_logger);
  }
}
#endif
}  // namespace noisepage::metrics
