#include "loggers/network_logger.h"

namespace noisepage::network {
#ifdef NOISEPAGE_USE_LOGGING
common::SanctionedSharedPtr<spdlog::logger>::Ptr network_logger = nullptr;

void InitNetworkLogger() {
  if (network_logger == nullptr) {
    network_logger = std::make_shared<spdlog::logger>("network_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(network_logger);
  }
}
#endif
}  // namespace noisepage::network
