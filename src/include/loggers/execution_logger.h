#pragma once

#include <memory>

#include "loggers/loggers_util.h"

#ifdef NOISEPAGE_USE_LOGGING

namespace noisepage::execution {
extern std::shared_ptr<spdlog::logger> execution_logger;  // NOLINT

void InitExecutionLogger();
}  // namespace noisepage::execution

#define EXECUTION_LOG_TRACE(...) ::noisepage::execution::execution_logger->trace(__VA_ARGS__)
#define EXECUTION_LOG_DEBUG(...) ::noisepage::execution::execution_logger->debug(__VA_ARGS__)
#define EXECUTION_LOG_INFO(...) ::noisepage::execution::execution_logger->info(__VA_ARGS__)
#define EXECUTION_LOG_WARN(...) ::noisepage::execution::execution_logger->warn(__VA_ARGS__)
#define EXECUTION_LOG_ERROR(...) ::noisepage::execution::execution_logger->error(__VA_ARGS__)

#else

#define EXECUTION_LOG_TRACE(...) ((void)0)
#define EXECUTION_LOG_DEBUG(...) ((void)0)
#define EXECUTION_LOG_INFO(...) ((void)0)
#define EXECUTION_LOG_WARN(...) ((void)0)
#define EXECUTION_LOG_ERROR(...) ((void)0)

#endif
