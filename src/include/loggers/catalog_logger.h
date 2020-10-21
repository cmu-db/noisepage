#pragma once

#include <memory>

#include "loggers/loggers_util.h"

#ifdef NOISEPAGE_USE_LOGGING

namespace terrier::catalog {
extern std::shared_ptr<spdlog::logger> catalog_logger;  // NOLINT

void InitCatalogLogger();
}  // namespace terrier::catalog

#define CATALOG_LOG_TRACE(...) ::terrier::catalog::catalog_logger->trace(__VA_ARGS__)
#define CATALOG_LOG_DEBUG(...) ::terrier::catalog::catalog_logger->debug(__VA_ARGS__)
#define CATALOG_LOG_INFO(...) ::terrier::catalog::catalog_logger->info(__VA_ARGS__)
#define CATALOG_LOG_WARN(...) ::terrier::catalog::catalog_logger->warn(__VA_ARGS__)
#define CATALOG_LOG_ERROR(...) ::terrier::catalog::catalog_logger->error(__VA_ARGS__)

#else

#define CATALOG_LOG_TRACE(...) ((void)0)
#define CATALOG_LOG_DEBUG(...) ((void)0)
#define CATALOG_LOG_INFO(...) ((void)0)
#define CATALOG_LOG_WARN(...) ((void)0)
#define CATALOG_LOG_ERROR(...) ((void)0)
#endif
