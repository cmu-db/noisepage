#pragma once

#include <memory>

#include "loggers/loggers_util.h"

#ifdef NOISEPAGE_USE_LOGGING

namespace noisepage::network {
extern std::shared_ptr<spdlog::logger> network_logger;  // NOLINT

void InitNetworkLogger();
}  // namespace noisepage::network

#define NETWORK_LOG_TRACE(...) ::noisepage::network::network_logger->trace(__VA_ARGS__)
#define NETWORK_LOG_DEBUG(...) ::noisepage::network::network_logger->debug(__VA_ARGS__)
#define NETWORK_LOG_INFO(...) ::noisepage::network::network_logger->info(__VA_ARGS__)
#define NETWORK_LOG_WARN(...) ::noisepage::network::network_logger->warn(__VA_ARGS__)
#define NETWORK_LOG_ERROR(...) ::noisepage::network::network_logger->error(__VA_ARGS__)

#else

#define NETWORK_LOG_TRACE(...) ((void)0)
#define NETWORK_LOG_DEBUG(...) ((void)0)
#define NETWORK_LOG_INFO(...) ((void)0)
#define NETWORK_LOG_WARN(...) ((void)0)
#define NETWORK_LOG_ERROR(...) ((void)0)

#endif
