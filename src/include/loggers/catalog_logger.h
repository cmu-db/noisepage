#pragma once

#include "loggers/loggers_util.h"

#ifdef NOISEPAGE_USE_LOGGING

namespace noisepage::catalog {
extern common::SanctionedSharedPtr<spdlog::logger>::Ptr catalog_logger;

void InitCatalogLogger();
}  // namespace noisepage::catalog

#define CATALOG_LOG_TRACE(...) ::noisepage::catalog::catalog_logger->trace(__VA_ARGS__)
#define CATALOG_LOG_DEBUG(...) ::noisepage::catalog::catalog_logger->debug(__VA_ARGS__)
#define CATALOG_LOG_INFO(...) ::noisepage::catalog::catalog_logger->info(__VA_ARGS__)
#define CATALOG_LOG_WARN(...) ::noisepage::catalog::catalog_logger->warn(__VA_ARGS__)
#define CATALOG_LOG_ERROR(...) ::noisepage::catalog::catalog_logger->error(__VA_ARGS__)

#else

#define CATALOG_LOG_TRACE(...) ((void)0)
#define CATALOG_LOG_DEBUG(...) ((void)0)
#define CATALOG_LOG_INFO(...) ((void)0)
#define CATALOG_LOG_WARN(...) ((void)0)
#define CATALOG_LOG_ERROR(...) ((void)0)
#endif
