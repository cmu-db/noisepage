#include "loggers/catalog_logger.h"

#include <memory>

namespace noisepage::catalog {
#ifdef NOISEPAGE_USE_LOGGING
std::shared_ptr<spdlog::logger> catalog_logger = nullptr;  // NOLINT

void InitCatalogLogger() {
  if (catalog_logger == nullptr) {
    catalog_logger = std::make_shared<spdlog::logger>("catalog_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(catalog_logger);
  }
}
#endif
}  // namespace noisepage::catalog
