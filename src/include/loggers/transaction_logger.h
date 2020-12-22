#pragma once

#include "loggers/loggers_util.h"

#ifdef NOISEPAGE_USE_LOGGING

namespace noisepage::transaction {
extern common::SanctionedSharedPtr<spdlog::logger>::Ptr transaction_logger;

void InitTransactionLogger();
}  // namespace noisepage::transaction

#define TXN_LOG_TRACE(...) ::noisepage::transaction::transaction_logger->trace(__VA_ARGS__)
#define TXN_LOG_DEBUG(...) ::noisepage::transaction::transaction_logger->debug(__VA_ARGS__)
#define TXN_LOG_INFO(...) ::noisepage::transaction::transaction_logger->info(__VA_ARGS__)
#define TXN_LOG_WARN(...) ::noisepage::transaction::transaction_logger->warn(__VA_ARGS__)
#define TXN_LOG_ERROR(...) ::noisepage::transaction::transaction_logger->error(__VA_ARGS__)

#else

#define TXN_LOG_TRACE(...) ((void)0)
#define TXN_LOG_DEBUG(...) ((void)0)
#define TXN_LOG_INFO(...) ((void)0)
#define TXN_LOG_WARN(...) ((void)0)
#define TXN_LOG_ERROR(...) ((void)0)

#endif
