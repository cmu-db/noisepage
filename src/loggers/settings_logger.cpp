#include "loggers/settings_logger.h"

namespace noisepage::settings {
#ifdef NOISEPAGE_USE_LOGGING
common::SanctionedSharedPtr<spdlog::logger>::Ptr settings_logger = nullptr;

void InitSettingsLogger() {
  if (settings_logger == nullptr) {
    settings_logger = std::make_shared<spdlog::logger>("settings_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(settings_logger);
  }
}
#endif
}  // namespace noisepage::settings
