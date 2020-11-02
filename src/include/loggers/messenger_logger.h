#pragma once

#include <memory>

#include "loggers/loggers_util.h"

<<<<<<< HEAD
namespace terrier::messenger {
extern std::shared_ptr<spdlog::logger> messenger_logger;  // NOLINT

void InitMessengerLogger();
}  // namespace terrier::messenger

#define MESSENGER_LOG_TRACE(...) ::terrier::messenger::messenger_logger->trace(__VA_ARGS__);

#define MESSENGER_LOG_DEBUG(...) ::terrier::messenger::messenger_logger->debug(__VA_ARGS__);

#define MESSENGER_LOG_INFO(...) ::terrier::messenger::messenger_logger->info(__VA_ARGS__);

#define MESSENGER_LOG_WARN(...) ::terrier::messenger::messenger_logger->warn(__VA_ARGS__);

#define MESSENGER_LOG_ERROR(...) ::terrier::messenger::messenger_logger->error(__VA_ARGS__);
=======
#ifdef NOISEPAGE_USE_LOGGING

namespace noisepage::messenger {
extern std::shared_ptr<spdlog::logger> messenger_logger;  // NOLINT

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
>>>>>>> 193244ce13033c9e65563c6e7d0ccdedd0eaf973
