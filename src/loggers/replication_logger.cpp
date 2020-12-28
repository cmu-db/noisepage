#include "loggers/replication_logger.h"

#include <memory>

namespace noisepage::replication {
#ifdef NOISEPAGE_USE_LOGGING
common::SanctionedSharedPtr<spdlog::logger>::Ptr replication_logger = nullptr;

void InitReplicationLogger() {
  if (replication_logger == nullptr) {
    replication_logger = std::make_shared<spdlog::logger>("replication_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(replication_logger);
  }
}
#endif
}  // namespace noisepage::replication
