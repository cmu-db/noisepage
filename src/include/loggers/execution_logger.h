#pragma once

#include <memory>
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/spdlog.h"

namespace terrier::execution {
extern std::shared_ptr<spdlog::logger> execution_logger;

void InitExecutionLogger();
}  // namespace terrier::execution

#define EXECUTION_LOG_TRACE(...) ::terrier::execution::execution_logger->trace(__VA_ARGS__);

#define EXECUTION_LOG_DEBUG(...) ::terrier::execution::execution_logger->debug(__VA_ARGS__);

#define EXECUTION_LOG_INFO(...) ::terrier::execution::execution_logger->info(__VA_ARGS__);

#define EXECUTION_LOG_WARN(...) ::terrier::execution::execution_logger->warn(__VA_ARGS__);

#define EXECUTION_LOG_ERROR(...) ::terrier::execution::execution_logger->error(__VA_ARGS__);
