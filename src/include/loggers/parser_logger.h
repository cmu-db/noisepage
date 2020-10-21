#pragma once

#include <memory>

#include "loggers/loggers_util.h"

#ifdef NOISEPAGE_USE_LOGGING

namespace terrier::parser {
extern std::shared_ptr<spdlog::logger> parser_logger;  // NOLINT

void InitParserLogger();
}  // namespace terrier::parser

#define PARSER_LOG_TRACE(...) ::terrier::parser::parser_logger->trace(__VA_ARGS__)
#define PARSER_LOG_DEBUG(...) ::terrier::parser::parser_logger->debug(__VA_ARGS__)
#define PARSER_LOG_INFO(...) ::terrier::parser::parser_logger->info(__VA_ARGS__)
#define PARSER_LOG_WARN(...) ::terrier::parser::parser_logger->warn(__VA_ARGS__)
#define PARSER_LOG_ERROR(...) ::terrier::parser::parser_logger->error(__VA_ARGS__)

#else

#define PARSER_LOG_TRACE(...) ((void)0)
#define PARSER_LOG_DEBUG(...) ((void)0)
#define PARSER_LOG_INFO(...) ((void)0)
#define PARSER_LOG_WARN(...) ((void)0)
#define PARSER_LOG_ERROR(...) ((void)0)

#endif
