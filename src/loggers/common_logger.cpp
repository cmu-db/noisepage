#include "loggers/common_logger.h"

#include <memory>

namespace terrier::common {

std::shared_ptr<spdlog::logger> common_logger = nullptr;  // NOLINT

void InitCommonLogger() {
  if (common_logger == nullptr) {
    common_logger = std::make_shared<spdlog::logger>("common_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(common_logger);
  }
}

}  // namespace terrier::common
