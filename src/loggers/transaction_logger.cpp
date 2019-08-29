#include "loggers/transaction_logger.h"
#include <memory>
#include "loggers/main_logger.h"

namespace terrier::transaction {

std::shared_ptr<spdlog::logger> transaction_logger;

void InitTransactionLogger() {
  transaction_logger = std::make_shared<spdlog::logger>("transaction_logger", ::default_sink);
  spdlog::register_logger(transaction_logger);
}

}  // namespace terrier::transaction
