#include "loggers/test_logger.h"
#include <memory>
#include "loggers/main_logger.h"

namespace terrier {

std::shared_ptr<spdlog::logger> test_logger;

void init_test_logger() {
  test_logger = std::make_shared<spdlog::logger>("test_logger", ::default_sink);
  spdlog::register_logger(test_logger);
}

}  // namespace terrier
