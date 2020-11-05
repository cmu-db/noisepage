#pragma once

#include <memory>

#include "loggers/loggers_util.h"

#ifdef NOISEPAGE_USE_LOGGING

namespace noisepage::model {
extern std::shared_ptr<spdlog::logger> model_server_logger;  // NOLINT

void InitModelServerLogger();
}  // namespace noisepage::model

#define MODEL_LOG_TRACE(...) ::noisepage::model::model_server_logger->trace(__VA_ARGS__);
#define MODEL_LOG_DEBUG(...) ::noisepage::model::model_server_logger->debug(__VA_ARGS__);
#define MODEL_LOG_INFO(...) ::noisepage::model::model_server_logger->info(__VA_ARGS__);
#define MODEL_LOG_WARN(...) ::noisepage::model::model_server_logger->warn(__VA_ARGS__);
#define MODEL_LOG_ERROR(...) ::noisepage::model::model_server_logger->error(__VA_ARGS__);

#else

#define MODEL_LOG_TRACE(...) ((void)0)
#define MODEL_LOG_DEBUG(...) ((void)0)
#define MODEL_LOG_INFO(...) ((void)0)
#define MODEL_LOG_WARN(...) ((void)0)
#define MODEL_LOG_ERROR(...) ((void)0)

#endif
