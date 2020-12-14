#pragma once

#include "loggers/loggers_util.h"

#ifdef NOISEPAGE_USE_LOGGING

namespace noisepage::metrics {
extern common::SanctionedSharedPtr<spdlog::logger>::Ptr metrics_logger;

void InitMetricsLogger();
}  // namespace noisepage::metrics

#define METRICS_LOG_TRACE(...) ::noisepage::metrics::metrics_logger->trace(__VA_ARGS__)
#define METRICS_LOG_DEBUG(...) ::noisepage::metrics::metrics_logger->debug(__VA_ARGS__)
#define METRICS_LOG_INFO(...) ::noisepage::metrics::metrics_logger->info(__VA_ARGS__)
#define METRICS_LOG_WARN(...) ::noisepage::metrics::metrics_logger->warn(__VA_ARGS__)
#define METRICS_LOG_ERROR(...) ::noisepage::metrics::metrics_logger->error(__VA_ARGS__)

#else

#define METRICS_LOG_TRACE(...) ((void)0)
#define METRICS_LOG_DEBUG(...) ((void)0)
#define METRICS_LOG_INFO(...) ((void)0)
#define METRICS_LOG_WARN(...) ((void)0)
#define METRICS_LOG_ERROR(...) ((void)0)

#endif
