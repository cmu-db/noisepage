#pragma once

#include <memory>

#include "loggers/loggers_util.h"

namespace terrier::metrics {
extern std::shared_ptr<spdlog::logger> metrics_logger;  // NOLINT

void InitMetricsLogger();
}  // namespace terrier::metrics

#define METRICS_LOG_TRACE(...) ::terrier::metrics::metrics_logger->trace(__VA_ARGS__);

#define METRICS_LOG_DEBUG(...) ::terrier::metrics::metrics_logger->debug(__VA_ARGS__);

#define METRICS_LOG_INFO(...) ::terrier::metrics::metrics_logger->info(__VA_ARGS__);

#define METRICS_LOG_WARN(...) ::terrier::metrics::metrics_logger->warn(__VA_ARGS__);

#define METRICS_LOG_ERROR(...) ::terrier::metrics::metrics_logger->error(__VA_ARGS__);
