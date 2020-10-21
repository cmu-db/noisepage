#pragma once

#include <memory>

#include "loggers/loggers_util.h"

#ifdef NOISEPAGE_USE_LOGGING

namespace terrier::storage {
extern std::shared_ptr<spdlog::logger> index_logger;  // NOLINT

void InitIndexLogger();
}  // namespace terrier::storage

#define INDEX_LOG_TRACE(...) ::terrier::storage::index_logger->trace(__VA_ARGS__)
#define INDEX_LOG_DEBUG(...) ::terrier::storage::index_logger->debug(__VA_ARGS__)
#define INDEX_LOG_INFO(...) ::terrier::storage::index_logger->info(__VA_ARGS__)
#define INDEX_LOG_WARN(...) ::terrier::storage::index_logger->warn(__VA_ARGS__)
#define INDEX_LOG_ERROR(...) ::terrier::storage::index_logger->error(__VA_ARGS__)

#else

#define INDEX_LOG_TRACE(...) ((void)0)
#define INDEX_LOG_DEBUG(...) ((void)0)
#define INDEX_LOG_INFO(...) ((void)0)
#define INDEX_LOG_WARN(...) ((void)0)
#define INDEX_LOG_ERROR(...) ((void)0)

#endif
