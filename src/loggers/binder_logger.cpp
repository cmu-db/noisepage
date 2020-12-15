#include "loggers/binder_logger.h"

namespace noisepage::binder {
#ifdef NOISEPAGE_USE_LOGGING
common::SanctionedSharedPtr<spdlog::logger>::Ptr binder_logger = nullptr;

void InitBinderLogger() {
  if (binder_logger == nullptr) {
    binder_logger = std::make_shared<spdlog::logger>("binder_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(binder_logger);
  }
}
#endif
}  // namespace noisepage::binder
