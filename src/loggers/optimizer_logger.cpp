#include <memory>

#include "loggers/optimizer_logger.h"
#include "loggers/main_logger.h"

namespace terrier::optimizer {

std::shared_ptr<spdlog::logger> optimizer_logger;

void init_optimizer_logger() {
  optimizer_logger = std::make_shared<spdlog::logger>("optimizer_logger", ::default_sink);
  spdlog::register_logger(optimizer_logger);
}

}  // namespace terrier::optimizer
