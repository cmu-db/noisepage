#pragma once

#include <memory>

#include "loggers/loggers_util.h"

#ifdef NOISEPAGE_USE_LOGGING

namespace noisepage::settings {
extern std::shared_ptr<spdlog::logger> settings_logger;  // NOLINT

void InitSettingsLogger();
}  // namespace noisepage::settings

#define SETTINGS_LOG_TRACE(...) ::noisepage::settings::settings_logger->trace(__VA_ARGS__)
#define SETTINGS_LOG_DEBUG(...) ::noisepage::settings::settings_logger->debug(__VA_ARGS__)
#define SETTINGS_LOG_INFO(...) ::noisepage::settings::settings_logger->info(__VA_ARGS__)
#define SETTINGS_LOG_WARN(...) ::noisepage::settings::settings_logger->warn(__VA_ARGS__)
#define SETTINGS_LOG_ERROR(...) ::noisepage::settings::settings_logger->error(__VA_ARGS__)

#else

#define SETTINGS_LOG_TRACE(...) ((void)0)
#define SETTINGS_LOG_DEBUG(...) ((void)0)
#define SETTINGS_LOG_INFO(...) ((void)0)
#define SETTINGS_LOG_WARN(...) ((void)0)
#define SETTINGS_LOG_ERROR(...) ((void)0)

#endif
