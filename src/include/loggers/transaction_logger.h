#pragma once

#include <memory>

#include "loggers/loggers_util.h"

#ifdef NOISEPAGE_USE_LOGGING

namespace terrier::transaction {
extern std::shared_ptr<spdlog::logger> transaction_logger;  // NOLINT

void InitTransactionLogger();
}  // namespace terrier::transaction

#define TXN_LOG_TRACE(...) ::terrier::transaction::transaction_logger->trace(__VA_ARGS__)
#define TXN_LOG_DEBUG(...) ::terrier::transaction::transaction_logger->debug(__VA_ARGS__)
#define TXN_LOG_INFO(...) ::terrier::transaction::transaction_logger->info(__VA_ARGS__)
#define TXN_LOG_WARN(...) ::terrier::transaction::transaction_logger->warn(__VA_ARGS__)
#define TXN_LOG_ERROR(...) ::terrier::transaction::transaction_logger->error(__VA_ARGS__)

#else

#define TXN_LOG_TRACE(...) ((void)0)
#define TXN_LOG_DEBUG(...) ((void)0)
#define TXN_LOG_INFO(...) ((void)0)
#define TXN_LOG_WARN(...) ((void)0)
#define TXN_LOG_ERROR(...) ((void)0)

#endif
