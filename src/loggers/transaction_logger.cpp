#include "loggers/transaction_logger.h"

namespace noisepage::transaction {
#ifdef NOISEPAGE_USE_LOGGING
common::SanctionedSharedPtr<spdlog::logger>::Ptr transaction_logger = nullptr;

void InitTransactionLogger() {
  if (transaction_logger == nullptr) {
    transaction_logger = std::make_shared<spdlog::logger>("transaction_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(transaction_logger);
  }
}
#endif
}  // namespace noisepage::transaction
