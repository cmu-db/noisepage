#pragma once

#include <memory>

#include "loggers/loggers_util.h"

#ifdef NOISEPAGE_USE_LOGGING

namespace terrier::metrics {
extern std::shared_ptr<spdlog::logger> metrics_logger;  // NOLINT

void InitMetricsLogger();
}  // namespace terrier::metrics

#define METRICS_LOG_TRACE(...) ::terrier::metrics::metrics_logger->trace(__VA_ARGS__)
#define METRICS_LOG_DEBUG(...) ::terrier::metrics::metrics_logger->debug(__VA_ARGS__)
#define METRICS_LOG_INFO(...) ::terrier::metrics::metrics_logger->info(__VA_ARGS__)
#define METRICS_LOG_WARN(...) ::terrier::metrics::metrics_logger->warn(__VA_ARGS__)
#define METRICS_LOG_ERROR(...) ::terrier::metrics::metrics_logger->error(__VA_ARGS__)

#else

#define METRICS_LOG_TRACE(...) ((void)0)
#define METRICS_LOG_DEBUG(...) ((void)0)
#define METRICS_LOG_INFO(...) ((void)0)
#define METRICS_LOG_WARN(...) ((void)0)
#define METRICS_LOG_ERROR(...) ((void)0)

#endif
