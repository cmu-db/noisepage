#include "loggers/parser_logger.h"

#include <memory>

namespace noisepage::parser {
#ifdef NOISEPAGE_USE_LOGGING
std::shared_ptr<spdlog::logger> parser_logger = nullptr;  // NOLINT

void InitParserLogger() {
  if (parser_logger == nullptr) {
    parser_logger = std::make_shared<spdlog::logger>("parser_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(parser_logger);
  }
}
#endif
}  // namespace noisepage::parser
