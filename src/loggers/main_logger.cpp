#include "loggers/main_logger.h"
#include <memory>

std::shared_ptr<spdlog::sinks::stdout_sink_mt> default_sink;  // NOLINT
std::shared_ptr<spdlog::logger> main_logger;  // NOLINT

void init_main_logger() {
  // create the default, shared sink
  default_sink = std::make_shared<spdlog::sinks::stdout_sink_mt>();  // NOLINT

  // the terrier, top level logger
  main_logger = std::make_shared<spdlog::logger>("main_logger", default_sink);  // NOLINT
  spdlog::register_logger(main_logger);
}
