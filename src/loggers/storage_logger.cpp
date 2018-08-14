//
// Created by pakhtar on 8/10/18.
//

#include <memory>
#include "loggers/main_logger.h"
#include "loggers/storage_logger.h"

namespace terrier::storage {

std::shared_ptr<spdlog::logger> storage_logger;

  void init_storage_logger() {
  storage_logger = std::make_shared<spdlog::logger>("storage_logger", ::default_sink);
  spdlog::register_logger(storage_logger);
}

}  // namespace terrier::storage
