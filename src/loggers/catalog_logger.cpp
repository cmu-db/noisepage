#include "loggers/catalog_logger.h"
#include <memory>
#include "loggers/main_logger.h"

namespace terrier::catalog {

std::shared_ptr<spdlog::logger> catalog_logger;

void InitCatalogLogger() {
  catalog_logger = std::make_shared<spdlog::logger>("catalog_logger", ::default_sink);
  spdlog::register_logger(catalog_logger);
  catalog_logger->set_level(spdlog::level::trace);
}

}  // namespace terrier::catalog
