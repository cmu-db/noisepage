#include "loggers/storage_logger.h"

namespace noisepage::storage {
#ifdef NOISEPAGE_USE_LOGGING
common::SanctionedSharedPtr<spdlog::logger>::Ptr storage_logger = nullptr;

void InitStorageLogger() {
  if (storage_logger == nullptr) {
    storage_logger = std::make_shared<spdlog::logger>("storage_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(storage_logger);
  }
}
#endif
}  // namespace noisepage::storage
