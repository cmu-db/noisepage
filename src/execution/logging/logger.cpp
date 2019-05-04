#include "execution/logging/logger.h"

#include <memory>

namespace tpl::logging {

std::shared_ptr<spdlog::sinks::stdout_sink_mt> default_sink;
std::shared_ptr<spdlog::logger> logger;

void InitLogger() {
  // Create the default, shared sink
  default_sink = std::make_shared<spdlog::sinks::stdout_sink_mt>();

  // The top level logger
  logger = std::make_shared<spdlog::logger>("logger", default_sink);
  // logger->set_level(SPD_LOG_LEVEL);

  spdlog::register_logger(logger);
}

void ShutdownLogger() { spdlog::shutdown(); }

}  // namespace tpl::logging
