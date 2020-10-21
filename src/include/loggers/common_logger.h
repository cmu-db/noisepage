#pragma once

#include <memory>

#include "loggers/loggers_util.h"

#ifdef NOISEPAGE_USE_LOGGING

namespace terrier::common {
extern std::shared_ptr<spdlog::logger> common_logger;  // NOLINT

void InitCommonLogger();
}  // namespace terrier::common

#define COMMON_LOG_TRACE(...) ::terrier::common::common_logger->trace(__VA_ARGS__)
#define COMMON_LOG_DEBUG(...) ::terrier::common::common_logger->debug(__VA_ARGS__)
#define COMMON_LOG_INFO(...) ::terrier::common::common_logger->info(__VA_ARGS__)
#define COMMON_LOG_WARN(...) ::terrier::common::common_logger->warn(__VA_ARGS__)
#define COMMON_LOG_ERROR(...) ::terrier::common::common_logger->error(__VA_ARGS__)

#else

#define COMMON_LOG_TRACE(...) ((void)0)
#define COMMON_LOG_DEBUG(...) ((void)0)
#define COMMON_LOG_INFO(...) ((void)0)
#define COMMON_LOG_WARN(...) ((void)0)
#define COMMON_LOG_ERROR(...) ((void)0)

#endif
