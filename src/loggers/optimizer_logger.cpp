#include "loggers/optimizer_logger.h"
#include <memory>
#include "loggers/main_logger.h"

namespace terrier::optimizer {

std::shared_ptr<spdlog::logger> optimizer_logger;  // NOLINT

void InitOptimizerLogger() {
  optimizer_logger = std::make_shared<spdlog::logger>("optimizer_logger", ::default_sink);  // NOLINT
  spdlog::register_logger(optimizer_logger);
  optimizer_logger->set_level(spdlog::level::trace);
}

}  // namespace terrier::optimizer
