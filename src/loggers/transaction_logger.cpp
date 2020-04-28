#include "loggers/transaction_logger.h"

#include <memory>

#include "loggers/loggers_util.h"
#include "spdlog/details/logger_impl.h"
#include "spdlog/logger.h"
#include "spdlog/spdlog.h"

namespace terrier::transaction {

std::shared_ptr<spdlog::logger> transaction_logger = nullptr;  // NOLINT

void InitTransactionLogger() {
  if (transaction_logger == nullptr) {
    transaction_logger = std::make_shared<spdlog::logger>("transaction_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(transaction_logger);
  }
}

}  // namespace terrier::transaction
