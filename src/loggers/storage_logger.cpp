#include "loggers/storage_logger.h"

#include <memory>

#include "loggers/loggers_util.h"
#include "spdlog/details/logger_impl.h"
#include "spdlog/logger.h"
#include "spdlog/spdlog.h"

namespace terrier::storage {

std::shared_ptr<spdlog::logger> storage_logger = nullptr;  // NOLINT

void InitStorageLogger() {
  if (storage_logger == nullptr) {
    storage_logger = std::make_shared<spdlog::logger>("storage_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(storage_logger);
  }
}

}  // namespace terrier::storage
