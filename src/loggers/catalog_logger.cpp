#include "loggers/catalog_logger.h"

#include <memory>

namespace terrier::catalog {

std::shared_ptr<spdlog::logger> catalog_logger = nullptr;  // NOLINT

void InitCatalogLogger() {
  if (catalog_logger == nullptr) {
    catalog_logger = std::make_shared<spdlog::logger>("catalog_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(catalog_logger);
  }
}

}  // namespace terrier::catalog
