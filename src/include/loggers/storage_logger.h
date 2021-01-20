#pragma once

#include "loggers/loggers_util.h"

#ifdef NOISEPAGE_USE_LOGGING

namespace noisepage::storage {
extern common::SanctionedSharedPtr<spdlog::logger>::Ptr storage_logger;

void InitStorageLogger();
}  // namespace noisepage::storage

#define STORAGE_LOG_TRACE(...) ::noisepage::storage::storage_logger->trace(__VA_ARGS__)
#define STORAGE_LOG_DEBUG(...) ::noisepage::storage::storage_logger->debug(__VA_ARGS__)
#define STORAGE_LOG_INFO(...) ::noisepage::storage::storage_logger->info(__VA_ARGS__)
#define STORAGE_LOG_WARN(...) ::noisepage::storage::storage_logger->warn(__VA_ARGS__)
#define STORAGE_LOG_ERROR(...) ::noisepage::storage::storage_logger->error(__VA_ARGS__)

#else

#define STORAGE_LOG_TRACE(...) ((void)0)
#define STORAGE_LOG_DEBUG(...) ((void)0)
#define STORAGE_LOG_INFO(...) ((void)0)
#define STORAGE_LOG_WARN(...) ((void)0)
#define STORAGE_LOG_ERROR(...) ((void)0)

#endif
