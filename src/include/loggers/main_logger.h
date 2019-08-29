#pragma once

#include <memory>
#include "spdlog/fmt/ostr.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

// flush the debug logs, every <n> seconds
#define DEBUG_LOG_FLUSH_INTERVAL 3

extern std::shared_ptr<spdlog::sinks::stdout_sink_mt> default_sink;
extern std::shared_ptr<spdlog::logger> main_logger;

void InitMainLogger();

#define LOG_TRACE(...) ::main_logger->trace(__VA_ARGS__);

#define LOG_DEBUG(...) ::main_logger->debug(__VA_ARGS__);

#define LOG_INFO(...) ::main_logger->info(__VA_ARGS__);

#define LOG_WARN(...) ::main_logger->warn(__VA_ARGS__);

#define LOG_ERROR(...) ::main_logger->error(__VA_ARGS__);
