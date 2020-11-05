#include "loggers/model_logger.h"

#include <memory>

namespace noisepage::model {
#ifdef NOISEPAGE_USE_LOGGING
std::shared_ptr<spdlog::logger> model_server_logger = nullptr;  // NOLINT

void InitModelServerLogger() {
  if (model_server_logger == nullptr) {
    model_server_logger = std::make_shared<spdlog::logger>("model_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(model_server_logger);
  }
}
#endif
}  // namespace noisepage::model
