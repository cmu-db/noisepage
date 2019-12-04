#include "loggers/main_logger.h"

#include <memory>

std::shared_ptr<spdlog::sinks::stdout_sink_mt> default_sink = nullptr;  // NOLINT
std::shared_ptr<spdlog::logger> main_logger = nullptr;                  // NOLINT

void InitMainLogger() {
  // create the default, shared sink
  if (default_sink == nullptr) {
    default_sink = std::make_shared<spdlog::sinks::stdout_sink_mt>();  // NOLINT
  }

  // the terrier, top level logger
  if (main_logger == nullptr) {
    main_logger = std::make_shared<spdlog::logger>("main_logger", default_sink);  // NOLINT
    spdlog::register_logger(main_logger);
  }
}
