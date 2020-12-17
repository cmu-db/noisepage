#pragma once

#include "loggers/loggers_util.h"

#ifdef NOISEPAGE_USE_LOGGING

namespace noisepage::optimizer {
extern common::SanctionedSharedPtr<spdlog::logger>::Ptr optimizer_logger;

void InitOptimizerLogger();
}  // namespace noisepage::optimizer

#define OPTIMIZER_LOG_TRACE(...) ::noisepage::optimizer::optimizer_logger->trace(__VA_ARGS__)
#define OPTIMIZER_LOG_DEBUG(...) ::noisepage::optimizer::optimizer_logger->debug(__VA_ARGS__)
#define OPTIMIZER_LOG_INFO(...) ::noisepage::optimizer::optimizer_logger->info(__VA_ARGS__)
#define OPTIMIZER_LOG_WARN(...) ::noisepage::optimizer::optimizer_logger->warn(__VA_ARGS__)
#define OPTIMIZER_LOG_ERROR(...) ::noisepage::optimizer::optimizer_logger->error(__VA_ARGS__)

#else

#define OPTIMIZER_LOG_TRACE(...) ((void)0)
#define OPTIMIZER_LOG_DEBUG(...) ((void)0)
#define OPTIMIZER_LOG_INFO(...) ((void)0)
#define OPTIMIZER_LOG_WARN(...) ((void)0)
#define OPTIMIZER_LOG_ERROR(...) ((void)0)

#endif
