#include "loggers/binder_logger.h"
#include <memory>
#include "loggers/main_logger.h"

namespace terrier::binder {

std::shared_ptr<spdlog::logger> binder_logger;

void InitBinderLogger() {
  binder_logger = std::make_shared<spdlog::logger>("binder_logger", ::default_sink);
  spdlog::register_logger(binder_logger);
  binder_logger->set_level(spdlog::level::trace);
}

}  // namespace terrier::binder
