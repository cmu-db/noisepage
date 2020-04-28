#pragma once

#include <memory>

#include "loggers/loggers_util.h"

namespace spdlog {
class logger;
}  // namespace spdlog

namespace terrier::binder {
extern std::shared_ptr<spdlog::logger> binder_logger;  // NOLINT

void InitBinderLogger();
}  // namespace terrier::binder

#define BINDER_LOG_TRACE(...) ::terrier::binder::binder_logger->trace(__VA_ARGS__);

#define BINDER_LOG_DEBUG(...) ::terrier::binder::binder_logger->debug(__VA_ARGS__);

#define BINDER_LOG_INFO(...) ::terrier::binder::binder_logger->info(__VA_ARGS__);

#define BINDER_LOG_WARN(...) ::terrier::binder::binder_logger->warn(__VA_ARGS__);

#define BINDER_LOG_ERROR(...) ::terrier::binder::binder_logger->error(__VA_ARGS__);
