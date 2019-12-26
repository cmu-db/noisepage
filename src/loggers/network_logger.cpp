#include "loggers/network_logger.h"

#include <memory>

namespace terrier::network {

std::shared_ptr<spdlog::logger> network_logger = nullptr;  // NOLINT

void InitNetworkLogger() {
  if (network_logger == nullptr) {
    network_logger = std::make_shared<spdlog::logger>("network_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(network_logger);
  }
}

}  // namespace terrier::network
