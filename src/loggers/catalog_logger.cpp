#include "loggers/catalog_logger.h"

namespace noisepage::catalog {
#ifdef NOISEPAGE_USE_LOGGING
common::SanctionedSharedPtr<spdlog::logger>::Ptr catalog_logger = nullptr;

void InitCatalogLogger() {
  if (catalog_logger == nullptr) {
    catalog_logger = std::make_shared<spdlog::logger>("catalog_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(catalog_logger);
  }
}
#endif
}  // namespace noisepage::catalog
