#pragma once

#include <string_view>

namespace noisepage::common {

// @see https://www.postgresql.org/docs/current/protocol-message-formats.html
enum class ErrorSeverity : uint8_t {
  ERROR,
  FATAL,
  PANIC,  // (in an error message)
  WARNING,
  NOTICE,
  DEBUG,
  INFO,
  LOG  // (in a notice message)
};

constexpr std::string_view ErrorSeverityToString(const common::ErrorSeverity severity) {
  switch (severity) {
    case ErrorSeverity::ERROR:
      return "ERROR";
    case ErrorSeverity::FATAL:
      return "FATAL";
    case ErrorSeverity::PANIC:
      return "PANIC";
    case ErrorSeverity::WARNING:
      return "WARNING";
    case ErrorSeverity::NOTICE:
      return "NOTICE";
    case ErrorSeverity::DEBUG:
      return "DEBUG";
    case ErrorSeverity::INFO:
      return "INFO";
    case ErrorSeverity::LOG:
      return "LOG";
    default:
      return "UNKNOWN";
  }
}

// @see https://www.postgresql.org/docs/current/protocol-error-fields.html
enum class ErrorField : unsigned char {
  SEVERITY = 'S',
  SEVERITY_LOCALIZED = 'V',  // Postgres 9.6 or later
  CODE = 'C',
  HUMAN_READABLE_ERROR = 'M',
  DETAIL = 'D',
  HINT = 'H',
  POSITION = 'P',
  INTERNAL_POSITION = 'p',
  INTERNAL_QUERY = 'q',
  WHERE = 'W',
  SCHEMA_NAME = 's',
  TABLE_NAME = 't',
  COLUMN_NAME = 'c',
  DATA_TYPE_NAME = 'd',
  CONSTRAINT_NAME = 'n',
  FILE = 'F',
  LINE = 'L',
  ROUTINE = 'R'
};

}  // namespace noisepage::common
