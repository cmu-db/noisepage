#include "loggers/network_logger.h"

#include <memory>

namespace noisepage::network {
#ifdef NOISEPAGE_USE_LOGGING
std::shared_ptr<spdlog::logger> network_logger = nullptr;  // NOLINT

void InitNetworkLogger() {
  if (network_logger == nullptr) {
    network_logger = std::make_shared<spdlog::logger>("network_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(network_logger);
  }
}
#endif
}  // namespace noisepage::network
