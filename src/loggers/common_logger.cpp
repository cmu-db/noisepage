#include "loggers/common_logger.h"

namespace noisepage::common {
#ifdef NOISEPAGE_USE_LOGGING
common::SanctionedSharedPtr<spdlog::logger>::Ptr common_logger = nullptr;

void InitCommonLogger() {
  if (common_logger == nullptr) {
    common_logger = std::make_shared<spdlog::logger>("common_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(common_logger);
  }
}
#endif
}  // namespace noisepage::common
