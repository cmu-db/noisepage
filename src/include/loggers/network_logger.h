#pragma once

#include <memory>
#include "spdlog/fmt/ostr.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/spdlog.h"

namespace terrier::network {
extern std::shared_ptr<spdlog::logger> network_logger;

void InitNetworkLogger();
}  // namespace terrier::network

#define NETWORK_LOG_TRACE(...) ::terrier::network::network_logger->trace(__VA_ARGS__);

#define NETWORK_LOG_DEBUG(...) ::terrier::network::network_logger->debug(__VA_ARGS__);

#define NETWORK_LOG_INFO(...) ::terrier::network::network_logger->info(__VA_ARGS__);

#define NETWORK_LOG_WARN(...) ::terrier::network::network_logger->warn(__VA_ARGS__);

#define NETWORK_LOG_ERROR(...) ::terrier::network::network_logger->error(__VA_ARGS__);
