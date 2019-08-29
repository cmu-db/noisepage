#include <memory>

#include "loggers/main_logger.h"
#include "loggers/optimizer_logger.h"

namespace terrier::optimizer {

std::shared_ptr<spdlog::logger> optimizer_logger;

void InitOptimizerLogger() {
  optimizer_logger = std::make_shared<spdlog::logger>("optimizer_logger", ::default_sink);
  spdlog::register_logger(optimizer_logger);
}

}  // namespace terrier::optimizer
