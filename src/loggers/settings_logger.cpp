#include "loggers/settings_logger.h"
#include <memory>
#include "loggers/main_logger.h"

namespace terrier::settings {

std::shared_ptr<spdlog::logger> settings_logger;

void InitSettingsLogger() {
  settings_logger = std::make_shared<spdlog::logger>("settings_logger", ::default_sink);
  spdlog::register_logger(settings_logger);
  settings_logger->set_level(spdlog::level::trace);
}

}  // namespace terrier::settings
