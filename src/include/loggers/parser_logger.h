#pragma once

#include <memory>
#include "spdlog/fmt/ostr.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/spdlog.h"

namespace terrier::parser {
extern std::shared_ptr<spdlog::logger> parser_logger;

void InitParserLogger();
}  // namespace terrier::parser

#define PARSER_LOG_TRACE(...) ::terrier::parser::parser_logger->trace(__VA_ARGS__);

#define PARSER_LOG_DEBUG(...) ::terrier::parser::parser_logger->debug(__VA_ARGS__);

#define PARSER_LOG_INFO(...) ::terrier::parser::parser_logger->info(__VA_ARGS__);

#define PARSER_LOG_WARN(...) ::terrier::parser::parser_logger->warn(__VA_ARGS__);

#define PARSER_LOG_ERROR(...) ::terrier::parser::parser_logger->error(__VA_ARGS__);
