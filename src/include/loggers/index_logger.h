#pragma once

#include <memory>
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/spdlog.h"

namespace terrier::storage {
extern std::shared_ptr<spdlog::logger> index_logger;  // NOLINT

void init_index_logger();
}  // namespace terrier::storage

#define INDEX_LOG_TRACE(...) ::terrier::storage::index_logger->trace(__VA_ARGS__);

#define INDEX_LOG_DEBUG(...) ::terrier::storage::index_logger->debug(__VA_ARGS__);

#define INDEX_LOG_INFO(...) ::terrier::storage::index_logger->info(__VA_ARGS__);

#define INDEX_LOG_WARN(...) ::terrier::storage::index_logger->warn(__VA_ARGS__);

#define INDEX_LOG_ERROR(...) ::terrier::storage::index_logger->error(__VA_ARGS__);
