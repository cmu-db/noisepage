#pragma once

#include <memory>

#include "loggers/loggers_util.h"

#ifdef NOISEPAGE_USE_LOGGING

namespace terrier::optimizer {
extern std::shared_ptr<spdlog::logger> optimizer_logger;  // NOLINT

void InitOptimizerLogger();
}  // namespace terrier::optimizer

#define OPTIMIZER_LOG_TRACE(...) ::terrier::optimizer::optimizer_logger->trace(__VA_ARGS__)
#define OPTIMIZER_LOG_DEBUG(...) ::terrier::optimizer::optimizer_logger->debug(__VA_ARGS__)
#define OPTIMIZER_LOG_INFO(...) ::terrier::optimizer::optimizer_logger->info(__VA_ARGS__)
#define OPTIMIZER_LOG_WARN(...) ::terrier::optimizer::optimizer_logger->warn(__VA_ARGS__)
#define OPTIMIZER_LOG_ERROR(...) ::terrier::optimizer::optimizer_logger->error(__VA_ARGS__)

#else

#define OPTIMIZER_LOG_TRACE(...) ((void)0)
#define OPTIMIZER_LOG_DEBUG(...) ((void)0)
#define OPTIMIZER_LOG_INFO(...) ((void)0)
#define OPTIMIZER_LOG_WARN(...) ((void)0)
#define OPTIMIZER_LOG_ERROR(...) ((void)0)

#endif
