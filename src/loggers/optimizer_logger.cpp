#include "loggers/optimizer_logger.h"

#include <memory>

#include "loggers/loggers_util.h"
#include "spdlog/details/logger_impl.h"
#include "spdlog/logger.h"
#include "spdlog/spdlog.h"

namespace terrier::optimizer {

std::shared_ptr<spdlog::logger> optimizer_logger = nullptr;  // NOLINT

void InitOptimizerLogger() {
  if (optimizer_logger == nullptr) {
    optimizer_logger = std::make_shared<spdlog::logger>("optimizer_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(optimizer_logger);
  }
}

}  // namespace terrier::optimizer
