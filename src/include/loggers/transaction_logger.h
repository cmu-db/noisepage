#pragma once

#include <memory>

#include "loggers/loggers_util.h"

namespace spdlog {
class logger;
}  // namespace spdlog

namespace terrier::transaction {
extern std::shared_ptr<spdlog::logger> transaction_logger;  // NOLINT

void InitTransactionLogger();
}  // namespace terrier::transaction

#define TXN_LOG_TRACE(...) ::terrier::transaction::transaction_logger->trace(__VA_ARGS__);

#define TXN_LOG_DEBUG(...) ::terrier::transaction::transaction_logger->debug(__VA_ARGS__);

#define TXN_LOG_INFO(...) ::terrier::transaction::transaction_logger->info(__VA_ARGS__);

#define TXN_LOG_WARN(...) ::terrier::transaction::transaction_logger->warn(__VA_ARGS__);

#define TXN_LOG_ERROR(...) ::terrier::transaction::transaction_logger->error(__VA_ARGS__);
