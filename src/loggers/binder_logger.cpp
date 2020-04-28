#include "loggers/binder_logger.h"

#include <memory>

#include "loggers/loggers_util.h"
#include "spdlog/details/logger_impl.h"
#include "spdlog/logger.h"
#include "spdlog/spdlog.h"

namespace terrier::binder {

std::shared_ptr<spdlog::logger> binder_logger = nullptr;  // NOLINT

void InitBinderLogger() {
  if (binder_logger == nullptr) {
    binder_logger = std::make_shared<spdlog::logger>("binder_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(binder_logger);
  }
}

}  // namespace terrier::binder
