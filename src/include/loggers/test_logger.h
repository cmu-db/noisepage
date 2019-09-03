#pragma once

#include <memory>
#include "spdlog/fmt/ostr.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/spdlog.h"

namespace terrier {
extern std::shared_ptr<spdlog::logger> test_logger;

void InitTestLogger();
}  // namespace terrier

#define TEST_LOG_TRACE(...) ::terrier::test_logger->trace(__VA_ARGS__);

#define TEST_LOG_DEBUG(...) ::terrier::test_logger->debug(__VA_ARGS__);

#define TEST_LOG_INFO(...) ::terrier::test_logger->info(__VA_ARGS__);

#define TEST_LOG_WARN(...) ::terrier::test_logger->warn(__VA_ARGS__);

#define TEST_LOG_ERROR(...) ::terrier::test_logger->error(__VA_ARGS__);
