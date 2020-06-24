#pragma once

#include <string>
#include <string_view>
#include <vector>

namespace terrier::common {

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

template <ErrorSeverity severity>
constexpr std::string_view ErrorSeverityToString() {
  if constexpr (severity == ErrorSeverity::ERROR) return "ERROR";
  if constexpr (severity == ErrorSeverity::FATAL) return "FATAL";
  if constexpr (severity == ErrorSeverity::PANIC) return "PANIC";
  if constexpr (severity == ErrorSeverity::WARNING) return "WARNING";
  if constexpr (severity == ErrorSeverity::NOTICE) return "NOTICE";
  if constexpr (severity == ErrorSeverity::DEBUG) return "DEBUG";
  if constexpr (severity == ErrorSeverity::INFO) return "INFO";
  if constexpr (severity == ErrorSeverity::LOG) return "LOG";
}

// @see https://www.postgresql.org/docs/current/protocol-error-fields.html
enum class ErrorField : unsigned char {
  SEVERITY = 'S',
  SEVERITY_LOCALIZED = 'V',  // Postgres 9.6 or later
  CODE = 'C',                // TODO(Matt): https://www.postgresql.org/docs/current/errcodes-appendix.html
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

}  // namespace terrier::common
