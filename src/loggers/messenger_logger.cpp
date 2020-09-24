#include "loggers/messenger_logger.h"

#include <memory>

namespace terrier::messenger {

std::shared_ptr<spdlog::logger> messenger_logger = nullptr;  // NOLINT

void InitMessengerLogger() {
  if (messenger_logger == nullptr) {
    messenger_logger = std::make_shared<spdlog::logger>("messenger_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(messenger_logger);
  }
}

}  // namespace terrier::messenger
