#include "loggers/execution_logger.h"

#include <memory>

namespace terrier::execution {
#ifdef NOISEPAGE_USE_LOGGING
std::shared_ptr<spdlog::logger> execution_logger = nullptr;  // NOLINT

void InitExecutionLogger() {
  if (execution_logger == nullptr) {
    execution_logger = std::make_shared<spdlog::logger>("execution_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(execution_logger);
  }
}
#endif
}  // namespace terrier::execution
