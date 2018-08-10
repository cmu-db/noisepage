//
// Created by pakhtar on 8/10/18.
//

#pragma once

#include "spdlog/spdlog.h"
#include "spdlog/sinks/basic_file_sink.h"

namespace terrier::storage {
  extern std::shared_ptr<spdlog::logger> storage_logger;

  void init_storage_logger();
}

#define STORAGE_LOG_TRACE(...) \
  ::terrier::storage::storage_logger->trace(__VA_ARGS__);

#define STORAGE_LOG_DEBUG(...) \
  ::terrier::storage::storage_logger->debug(__VA_ARGS__);

#define STORAGE_LOG_INFO(...) \
  ::terrier::storage::storage_logger->info(__VA_ARGS__);

#define STORAGE_LOG_WARN(...) \
  ::terrier::storage::storage`_logger->warn(__VA_ARGS__);

#define STORAGE_LOG_ERROR(...) \
  ::terrier::storage::storage_logger->error(__VA_ARGS__);
