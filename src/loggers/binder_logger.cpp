#include "loggers/binder_logger.h"

#include <memory>

namespace terrier::binder {
#ifdef NOISEPAGE_USE_LOGGING
std::shared_ptr<spdlog::logger> binder_logger = nullptr;  // NOLINT

void InitBinderLogger() {
  if (binder_logger == nullptr) {
    binder_logger = std::make_shared<spdlog::logger>("binder_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(binder_logger);
  }
}
#endif
}  // namespace terrier::binder
