#include "loggers/settings_logger.h"

#include <memory>

namespace terrier::settings {

std::shared_ptr<spdlog::logger> settings_logger = nullptr;  // NOLINT

void InitSettingsLogger() {
  if (settings_logger == nullptr) {
    settings_logger = std::make_shared<spdlog::logger>("settings_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(settings_logger);
  }
}

}  // namespace terrier::settings
