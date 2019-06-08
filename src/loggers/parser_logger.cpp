#include "loggers/parser_logger.h"
#include <memory>
#include "loggers/main_logger.h"

namespace terrier::parser {

std::shared_ptr<spdlog::logger> parser_logger;  // NOLINT

void init_parser_logger() {
  parser_logger = std::make_shared<spdlog::logger>("parser_logger", ::default_sink);  // NOLINT
  spdlog::register_logger(parser_logger);
}

}  // namespace terrier::parser
