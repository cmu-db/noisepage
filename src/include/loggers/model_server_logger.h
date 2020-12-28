#pragma once

#include <memory>

#include "loggers/loggers_util.h"

#ifdef NOISEPAGE_USE_LOGGING

namespace noisepage::modelserver {
extern std::shared_ptr<spdlog::logger> model_server_logger;  // NOLINT

void InitModelServerLogger();
}  // namespace noisepage::modelserver

#define MODEL_SERVER_LOG_TRACE(...) ::noisepage::modelserver::model_server_logger->trace(__VA_ARGS__);
#define MODEL_SERVER_LOG_DEBUG(...) ::noisepage::modelserver::model_server_logger->debug(__VA_ARGS__);
#define MODEL_SERVER_LOG_INFO(...) ::noisepage::modelserver::model_server_logger->info(__VA_ARGS__);
#define MODEL_SERVER_LOG_WARN(...) ::noisepage::modelserver::model_server_logger->warn(__VA_ARGS__);
#define MODEL_SERVER_LOG_ERROR(...) ::noisepage::modelserver::model_server_logger->error(__VA_ARGS__);

#else

#define MODEL_SERVER_LOG_TRACE(...) ((void)0)
#define MODEL_SERVER_LOG_DEBUG(...) ((void)0)
#define MODEL_SERVER_LOG_INFO(...) ((void)0)
#define MODEL_SERVER_LOG_WARN(...) ((void)0)
#define MODEL_SERVER_LOG_ERROR(...) ((void)0)

#endif
