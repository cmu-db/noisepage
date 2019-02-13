#include "loggers/type_logger.h"
#include <memory>
#include "loggers/main_logger.h"

namespace terrier::type {

std::shared_ptr<spdlog::logger> type_logger;

void init_type_logger() {
  type_logger = std::make_shared<spdlog::logger>("type_logger", ::default_sink);
  spdlog::register_logger(type_logger);
}

}  // namespace terrier::type
