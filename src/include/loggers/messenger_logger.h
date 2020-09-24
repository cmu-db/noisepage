#pragma once

#include <memory>

#include "loggers/loggers_util.h"

namespace terrier::messenger {
extern std::shared_ptr<spdlog::logger> messenger_logger;  // NOLINT

void InitMessengerLogger();
}  // namespace terrier::messenger

#define MESSENGER_LOG_TRACE(...) ::terrier::messenger::messenger_logger->trace(__VA_ARGS__);

#define MESSENGER_LOG_DEBUG(...) ::terrier::messenger::messenger_logger->debug(__VA_ARGS__);

#define MESSENGER_LOG_INFO(...) ::terrier::messenger::messenger_logger->info(__VA_ARGS__);

#define MESSENGER_LOG_WARN(...) ::terrier::messenger::messenger_logger->warn(__VA_ARGS__);

#define MESSENGER_LOG_ERROR(...) ::terrier::messenger::messenger_logger->error(__VA_ARGS__);
