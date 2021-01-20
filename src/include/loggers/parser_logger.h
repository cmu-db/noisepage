#pragma once

#include "loggers/loggers_util.h"

#ifdef NOISEPAGE_USE_LOGGING

namespace noisepage::parser {
extern common::SanctionedSharedPtr<spdlog::logger>::Ptr parser_logger;

void InitParserLogger();
}  // namespace noisepage::parser

#define PARSER_LOG_TRACE(...) ::noisepage::parser::parser_logger->trace(__VA_ARGS__)
#define PARSER_LOG_DEBUG(...) ::noisepage::parser::parser_logger->debug(__VA_ARGS__)
#define PARSER_LOG_INFO(...) ::noisepage::parser::parser_logger->info(__VA_ARGS__)
#define PARSER_LOG_WARN(...) ::noisepage::parser::parser_logger->warn(__VA_ARGS__)
#define PARSER_LOG_ERROR(...) ::noisepage::parser::parser_logger->error(__VA_ARGS__)

#else

#define PARSER_LOG_TRACE(...) ((void)0)
#define PARSER_LOG_DEBUG(...) ((void)0)
#define PARSER_LOG_INFO(...) ((void)0)
#define PARSER_LOG_WARN(...) ((void)0)
#define PARSER_LOG_ERROR(...) ((void)0)

#endif
