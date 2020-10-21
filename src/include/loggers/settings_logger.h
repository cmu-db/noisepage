#pragma once

#include <memory>

#include "loggers/loggers_util.h"

#ifdef NOISEPAGE_USE_LOGGING

namespace terrier::settings {
extern std::shared_ptr<spdlog::logger> settings_logger;  // NOLINT

void InitSettingsLogger();
}  // namespace terrier::settings

#define SETTINGS_LOG_TRACE(...) ::terrier::settings::settings_logger->trace(__VA_ARGS__)
#define SETTINGS_LOG_DEBUG(...) ::terrier::settings::settings_logger->debug(__VA_ARGS__)
#define SETTINGS_LOG_INFO(...) ::terrier::settings::settings_logger->info(__VA_ARGS__)
#define SETTINGS_LOG_WARN(...) ::terrier::settings::settings_logger->warn(__VA_ARGS__)
#define SETTINGS_LOG_ERROR(...) ::terrier::settings::settings_logger->error(__VA_ARGS__)

#else

#define SETTINGS_LOG_TRACE(...) ((void)0)
#define SETTINGS_LOG_DEBUG(...) ((void)0)
#define SETTINGS_LOG_INFO(...) ((void)0)
#define SETTINGS_LOG_WARN(...) ((void)0)
#define SETTINGS_LOG_ERROR(...) ((void)0)

#endif
