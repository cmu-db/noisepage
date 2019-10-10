#include "loggers/storage_logger.h"
#include <memory>
#include "loggers/main_logger.h"

namespace terrier::storage {

std::shared_ptr<spdlog::logger> storage_logger;  // NOLINT

void InitStorageLogger() {
  storage_logger = std::make_shared<spdlog::logger>("storage_logger", ::default_sink);  // NOLINT
  spdlog::register_logger(storage_logger);
}

}  // namespace terrier::storage
