#include "loggers/storage_logger.h"

#include <memory>

namespace noisepage::storage {
#ifdef NOISEPAGE_USE_LOGGING
std::shared_ptr<spdlog::logger> storage_logger = nullptr;  // NOLINT

void InitStorageLogger() {
  if (storage_logger == nullptr) {
    storage_logger = std::make_shared<spdlog::logger>("storage_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(storage_logger);
  }
}
#endif
}  // namespace noisepage::storage
