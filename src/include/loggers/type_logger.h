#pragma once

#include <memory>
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/spdlog.h"

namespace terrier::type {
extern std::shared_ptr<spdlog::logger> type_logger;

void init_type_logger();
}  // namespace terrier::type

#define TYPE_LOG_TRACE(...) ::terrier::type::type_logger->trace(__VA_ARGS__);

#define TYPE_LOG_DEBUG(...) ::terrier::type::type_logger->debug(__VA_ARGS__);

#define TYPE_LOG_INFO(...) ::terrier::type::type_logger->info(__VA_ARGS__);

#define TYPE_LOG_WARN(...) ::terrier::type::type_logger->warn(__VA_ARGS__);

#define TYPE_LOG_ERROR(...) ::terrier::type::type_logger->error(__VA_ARGS__);
