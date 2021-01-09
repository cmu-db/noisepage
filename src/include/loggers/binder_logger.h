#pragma once

#include "loggers/loggers_util.h"

#ifdef NOISEPAGE_USE_LOGGING

namespace noisepage::binder {
extern common::SanctionedSharedPtr<spdlog::logger>::Ptr binder_logger;

void InitBinderLogger();
}  // namespace noisepage::binder

#define BINDER_LOG_TRACE(...) ::noisepage::binder::binder_logger->trace(__VA_ARGS__)
#define BINDER_LOG_DEBUG(...) ::noisepage::binder::binder_logger->debug(__VA_ARGS__)
#define BINDER_LOG_INFO(...) ::noisepage::binder::binder_logger->info(__VA_ARGS__)
#define BINDER_LOG_WARN(...) ::noisepage::binder::binder_logger->warn(__VA_ARGS__)
#define BINDER_LOG_ERROR(...) ::noisepage::binder::binder_logger->error(__VA_ARGS__)

#else

#define BINDER_LOG_TRACE(...) ((void)0)
#define BINDER_LOG_DEBUG(...) ((void)0)
#define BINDER_LOG_INFO(...) ((void)0)
#define BINDER_LOG_WARN(...) ((void)0)
#define BINDER_LOG_ERROR(...) ((void)0)

#endif
