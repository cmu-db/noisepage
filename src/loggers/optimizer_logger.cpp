#include "loggers/optimizer_logger.h"

namespace noisepage::optimizer {
#ifdef NOISEPAGE_USE_LOGGING
common::SanctionedSharedPtr<spdlog::logger>::Ptr optimizer_logger = nullptr;

void InitOptimizerLogger() {
  if (optimizer_logger == nullptr) {
    optimizer_logger = std::make_shared<spdlog::logger>("optimizer_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(optimizer_logger);
  }
}
#endif
}  // namespace noisepage::optimizer
