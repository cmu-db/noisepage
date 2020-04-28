#include "loggers/index_logger.h"

#include <memory>

#include "loggers/loggers_util.h"
#include "spdlog/details/logger_impl.h"
#include "spdlog/logger.h"
#include "spdlog/spdlog.h"

namespace terrier::storage {

std::shared_ptr<spdlog::logger> index_logger = nullptr;  // NOLINT

void InitIndexLogger() {
  if (index_logger == nullptr) {
    index_logger = std::make_shared<spdlog::logger>("index_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(index_logger);
  }
}
}  // namespace terrier::storage
