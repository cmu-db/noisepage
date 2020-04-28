#include "loggers/settings_logger.h"

#include <memory>

#include "loggers/loggers_util.h"
#include "spdlog/details/logger_impl.h"
#include "spdlog/logger.h"
#include "spdlog/spdlog.h"

namespace terrier::settings {

std::shared_ptr<spdlog::logger> settings_logger = nullptr;  // NOLINT

void InitSettingsLogger() {
  if (settings_logger == nullptr) {
    settings_logger = std::make_shared<spdlog::logger>("settings_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(settings_logger);
  }
}

}  // namespace terrier::settings
