#include <memory>
#include "loggers/main_logger.h"

namespace terrier::execution {

std::shared_ptr<spdlog::logger> execution_logger;  // NOLINT

void InitExecutionLogger() {
  execution_logger = std::make_shared<spdlog::logger>("execution_logger", ::default_sink);  // NOLINT
  spdlog::register_logger(execution_logger);
}

}  // namespace terrier::execution
