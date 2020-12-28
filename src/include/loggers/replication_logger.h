#pragma once

#include <memory>

#include "loggers/loggers_util.h"

#ifdef NOISEPAGE_USE_LOGGING

namespace noisepage::replication {
extern common::SanctionedSharedPtr<spdlog::logger>::Ptr replication_logger;

void InitReplicationLogger();
}  // namespace noisepage::replication

#define REPLICATION_LOG_TRACE(...) ::noisepage::replication::replication_logger->trace(__VA_ARGS__);
#define REPLICATION_LOG_DEBUG(...) ::noisepage::replication::replication_logger->debug(__VA_ARGS__);
#define REPLICATION_LOG_INFO(...) ::noisepage::replication::replication_logger->info(__VA_ARGS__);
#define REPLICATION_LOG_WARN(...) ::noisepage::replication::replication_logger->warn(__VA_ARGS__);
#define REPLICATION_LOG_ERROR(...) ::noisepage::replication::replication_logger->error(__VA_ARGS__);

#else

#define REPLICATION_LOG_TRACE(...) ((void)0)
#define REPLICATION_LOG_DEBUG(...) ((void)0)
#define REPLICATION_LOG_INFO(...) ((void)0)
#define REPLICATION_LOG_WARN(...) ((void)0)
#define REPLICATION_LOG_ERROR(...) ((void)0)

#endif
