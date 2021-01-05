#include "loggers/parser_logger.h"

namespace noisepage::parser {
#ifdef NOISEPAGE_USE_LOGGING
common::SanctionedSharedPtr<spdlog::logger>::Ptr parser_logger = nullptr;

void InitParserLogger() {
  if (parser_logger == nullptr) {
    parser_logger = std::make_shared<spdlog::logger>("parser_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(parser_logger);
  }
}
#endif
}  // namespace noisepage::parser
