#pragma once

#include <memory>

#include "loggers/loggers_util.h"

#ifdef NOISEPAGE_USE_LOGGING

namespace terrier::storage {
extern std::shared_ptr<spdlog::logger> storage_logger;  // NOLINT

void InitStorageLogger();
}  // namespace terrier::storage

#define STORAGE_LOG_TRACE(...) ::terrier::storage::storage_logger->trace(__VA_ARGS__)
#define STORAGE_LOG_DEBUG(...) ::terrier::storage::storage_logger->debug(__VA_ARGS__)
#define STORAGE_LOG_INFO(...) ::terrier::storage::storage_logger->info(__VA_ARGS__)
#define STORAGE_LOG_WARN(...) ::terrier::storage::storage_logger->warn(__VA_ARGS__)
#define STORAGE_LOG_ERROR(...) ::terrier::storage::storage_logger->error(__VA_ARGS__)

#else

#define STORAGE_LOG_TRACE(...) ((void)0)
#define STORAGE_LOG_DEBUG(...) ((void)0)
#define STORAGE_LOG_INFO(...) ((void)0)
#define STORAGE_LOG_WARN(...) ((void)0)
#define STORAGE_LOG_ERROR(...) ((void)0)

#endif
