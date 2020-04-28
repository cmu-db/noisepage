#include "loggers/execution_logger.h"

#include <memory>

#include "loggers/loggers_util.h"
#include "spdlog/details/logger_impl.h"
#include "spdlog/logger.h"
#include "spdlog/spdlog.h"

namespace terrier::execution {

std::shared_ptr<spdlog::logger> execution_logger = nullptr;  // NOLINT

void InitExecutionLogger() {
  if (execution_logger == nullptr) {
    execution_logger = std::make_shared<spdlog::logger>("execution_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(execution_logger);
  }
}

}  // namespace terrier::execution
