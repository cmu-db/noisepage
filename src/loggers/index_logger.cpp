#include "loggers/index_logger.h"

#include <memory>

namespace terrier::storage {
#ifdef NOISEPAGE_USE_LOGGING
std::shared_ptr<spdlog::logger> index_logger = nullptr;  // NOLINT

void InitIndexLogger() {
  if (index_logger == nullptr) {
    index_logger = std::make_shared<spdlog::logger>("index_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(index_logger);
  }
}
#endif
}  // namespace terrier::storage
