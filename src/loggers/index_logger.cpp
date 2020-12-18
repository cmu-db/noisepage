#include "loggers/index_logger.h"

namespace noisepage::storage {
#ifdef NOISEPAGE_USE_LOGGING
common::SanctionedSharedPtr<spdlog::logger>::Ptr index_logger = nullptr;

void InitIndexLogger() {
  if (index_logger == nullptr) {
    index_logger = std::make_shared<spdlog::logger>("index_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(index_logger);
  }
}
#endif
}  // namespace noisepage::storage
