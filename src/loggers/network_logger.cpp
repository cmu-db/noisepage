#include "loggers/network_logger.h"
#include <iostream>
#include <memory>
#include "loggers/main_logger.h"

namespace terrier::network {

std::shared_ptr<spdlog::logger> network_logger;

void InitNetworkLogger() {
  network_logger = std::make_shared<spdlog::logger>("network_logger", ::default_sink);
  spdlog::register_logger(network_logger);
}

}  // namespace terrier::network
