#include "loggers/index_logger.h"
#include <memory>
#include "loggers/main_logger.h"

namespace terrier::storage {

std::shared_ptr<spdlog::logger> index_logger;  // NOLINT

void InitIndexLogger() {
  index_logger = std::make_shared<spdlog::logger>("index_logger", ::default_sink);  // NOLINT
  spdlog::register_logger(index_logger);
}

}  // namespace terrier::storage
