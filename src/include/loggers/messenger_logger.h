#pragma once

#include "loggers/loggers_util.h"

#ifdef NOISEPAGE_USE_LOGGING

namespace noisepage::messenger {
extern common::SanctionedSharedPtr<spdlog::logger>::Ptr messenger_logger;

void InitMessengerLogger();
}  // namespace noisepage::messenger

#define MESSENGER_LOG_TRACE(...) ::noisepage::messenger::messenger_logger->trace(__VA_ARGS__);
#define MESSENGER_LOG_DEBUG(...) ::noisepage::messenger::messenger_logger->debug(__VA_ARGS__);
#define MESSENGER_LOG_INFO(...) ::noisepage::messenger::messenger_logger->info(__VA_ARGS__);
#define MESSENGER_LOG_WARN(...) ::noisepage::messenger::messenger_logger->warn(__VA_ARGS__);
#define MESSENGER_LOG_ERROR(...) ::noisepage::messenger::messenger_logger->error(__VA_ARGS__);

#else

#define MESSENGER_LOG_TRACE(...) ((void)0)
#define MESSENGER_LOG_DEBUG(...) ((void)0)
#define MESSENGER_LOG_INFO(...) ((void)0)
#define MESSENGER_LOG_WARN(...) ((void)0)
#define MESSENGER_LOG_ERROR(...) ((void)0)

#endif
