#include "loggers/optimizer_logger.h"

#include <memory>

namespace terrier::optimizer {
#ifdef NOISEPAGE_USE_LOGGING
std::shared_ptr<spdlog::logger> optimizer_logger = nullptr;  // NOLINT

void InitOptimizerLogger() {
  if (optimizer_logger == nullptr) {
    optimizer_logger = std::make_shared<spdlog::logger>("optimizer_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(optimizer_logger);
  }
}
#endif
}  // namespace terrier::optimizer
