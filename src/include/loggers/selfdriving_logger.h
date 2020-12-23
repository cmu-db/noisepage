#pragma once

#include "loggers/loggers_util.h"

#ifdef NOISEPAGE_USE_LOGGING

namespace noisepage::selfdriving {
extern common::SanctionedSharedPtr<spdlog::logger>::Ptr selfdriving_logger;

void InitSelfDrivingLogger();
}  // namespace noisepage::selfdriving

#define SELFDRIVING_LOG_TRACE(...) ::noisepage::selfdriving::selfdriving_logger->trace(__VA_ARGS__);
#define SELFDRIVING_LOG_DEBUG(...) ::noisepage::selfdriving::selfdriving_logger->debug(__VA_ARGS__);
#define SELFDRIVING_LOG_INFO(...) ::noisepage::selfdriving::selfdriving_logger->info(__VA_ARGS__);
#define SELFDRIVING_LOG_WARN(...) ::noisepage::selfdriving::selfdriving_logger->warn(__VA_ARGS__);
#define SELFDRIVING_LOG_ERROR(...) ::noisepage::selfdriving::selfdriving_logger->error(__VA_ARGS__);

#else

#define SELFDRIVING_LOG_TRACE(...) ((void)0)
#define SELFDRIVING_LOG_DEBUG(...) ((void)0)
#define SELFDRIVING_LOG_INFO(...) ((void)0)
#define SELFDRIVING_LOG_WARN(...) ((void)0)
#define SELFDRIVING_LOG_ERROR(...) ((void)0)

#endif
