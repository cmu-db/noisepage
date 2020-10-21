#pragma once

#include <memory>

#include "loggers/loggers_util.h"

#ifdef NOISEPAGE_USE_LOGGING

namespace terrier::execution {
extern std::shared_ptr<spdlog::logger> execution_logger;  // NOLINT

void InitExecutionLogger();
}  // namespace terrier::execution

#define EXECUTION_LOG_TRACE(...) ::terrier::execution::execution_logger->trace(__VA_ARGS__)
#define EXECUTION_LOG_DEBUG(...) ::terrier::execution::execution_logger->debug(__VA_ARGS__)
#define EXECUTION_LOG_INFO(...) ::terrier::execution::execution_logger->info(__VA_ARGS__)
#define EXECUTION_LOG_WARN(...) ::terrier::execution::execution_logger->warn(__VA_ARGS__)
#define EXECUTION_LOG_ERROR(...) ::terrier::execution::execution_logger->error(__VA_ARGS__)

#else

#define EXECUTION_LOG_TRACE(...) ((void)0)
#define EXECUTION_LOG_DEBUG(...) ((void)0)
#define EXECUTION_LOG_INFO(...) ((void)0)
#define EXECUTION_LOG_WARN(...) ((void)0)
#define EXECUTION_LOG_ERROR(...) ((void)0)

#endif
