#pragma once

#include <memory>
#include "spdlog/fmt/ostr.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/spdlog.h"

namespace terrier::catalog {
extern std::shared_ptr<spdlog::logger> catalog_logger;

void InitCatalogLogger();
}  // namespace terrier::catalog

#define CATALOG_LOG_TRACE(...) ::terrier::catalog::catalog_logger->trace(__VA_ARGS__);

#define CATALOG_LOG_DEBUG(...) ::terrier::catalog::catalog_logger->debug(__VA_ARGS__);

#define CATALOG_LOG_INFO(...) ::terrier::catalog::catalog_logger->info(__VA_ARGS__);

#define CATALOG_LOG_WARN(...) ::terrier::catalog::catalog_logger->warn(__VA_ARGS__);

#define CATALOG_LOG_ERROR(...) ::terrier::catalog::catalog_logger->error(__VA_ARGS__);
