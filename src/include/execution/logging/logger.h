#pragma once

#include <memory>

#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

namespace tpl::logging {

extern std::shared_ptr<spdlog::sinks::stdout_sink_mt> default_sink;
extern std::shared_ptr<spdlog::logger> logger;

/// Initialize/shutdown all loggers
void InitLogger();
void ShutdownLogger();

// ---------------------------------------------------------
// Define logging level
// ---------------------------------------------------------

#if defined(LOG_LEVEL_TRACE)
#define SPD_LOG_LEVEL spdlog::level::level_enum::trace
#elif defined(LOG_LEVEL_DEBUG)
#define SPD_LOG_LEVEL spdlog::level::level_enum::debug
#elif defined(LOG_LEVEL_INFO)
#define SPD_LOG_LEVEL spdlog::level::level_enum::info
#elif defined(LOG_LEVEL_WARN)
#define SPD_LOG_LEVEL spdlog::level::level_enum::warn
#elif defined(LOG_LEVEL_OFF)
#define SPD_LOG_LEVEL spdlog::level::level_enum::off
#else

// LOG_LEVEL_* not explicitly defined, use debug if in DEBUG mode, otherwise ERR
#ifndef NDEBUG
#define SPD_LOG_LEVEL spdlog::level::level_enum::debug
#else
#define SPD_LOG_LEVEL spdlog::level::level_enum::err
#endif

#endif

// ---------------------------------------------------------
// Logging macros
// ---------------------------------------------------------

#define LOG_TRACE(...) ::tpl::logging::logger->trace(__VA_ARGS__);

#define LOG_DEBUG(...) ::tpl::logging::logger->debug(__VA_ARGS__);

#define LOG_INFO(...) ::tpl::logging::logger->info(__VA_ARGS__);

#define LOG_WARN(...) ::tpl::logging::logger->warn(__VA_ARGS__);

#define LOG_ERROR(...) ::tpl::logging::logger->error(__VA_ARGS__);

}  // namespace tpl::logging
