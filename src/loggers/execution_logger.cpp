#include "loggers/execution_logger.h"

namespace noisepage::execution {
#ifdef NOISEPAGE_USE_LOGGING
common::SanctionedSharedPtr<spdlog::logger>::Ptr execution_logger = nullptr;

void InitExecutionLogger() {
  if (execution_logger == nullptr) {
    execution_logger = std::make_shared<spdlog::logger>("execution_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(execution_logger);
  }
}
#endif
}  // namespace noisepage::execution
