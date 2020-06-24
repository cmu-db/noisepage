#pragma once

#include <string>
#include <unordered_map>

#include "common/macros.h"
#include "network/network_defs.h"
#include "type/type_id.h"

namespace terrier::network {

/**
 * Hardcoded server parameter values to send to the client
 */
// clang-format off
const std::unordered_map<std::string, std::string> PG_PARAMETER_STATUS_MAP = {
    {"application_name", "psql"},
    {"client_encoding", "UTF8"},
    {"DateStyle", "ISO, MDY"},
    {"integer_datetimes", "on"},
    {"IntervalStyle", "postgres"},
    {"is_superuser", "on"},
    {"server_encoding", "UTF8"},
    {"server_version", "9.5devel"},
    {"session_authorization", "terrier"},
    {"standard_conforming_strings", "on"},
    {"TimeZone", "US/Eastern"}
};
// clang-format on

// @see https://www.postgresql.org/docs/current/protocol-message-formats.html
enum class PostgresSeverity : uint8_t {
  ERROR,
  FATAL,
  PANIC,  // (in an error message)
  WARNING,
  NOTICE,
  DEBUG,
  INFO,
  LOG  // (in a notice message)
};

template <PostgresSeverity severity>
constexpr std::string_view PostgresSeverityToString() {
  if constexpr (severity == PostgresSeverity::ERROR) return "ERROR";
  if constexpr (severity == PostgresSeverity::FATAL) return "FATAL";
  if constexpr (severity == PostgresSeverity::PANIC) return "PANIC";
  if constexpr (severity == PostgresSeverity::WARNING) return "WARNING";
  if constexpr (severity == PostgresSeverity::NOTICE) return "NOTICE";
  if constexpr (severity == PostgresSeverity::DEBUG) return "DEBUG";
  if constexpr (severity == PostgresSeverity::INFO) return "INFO";
  if constexpr (severity == PostgresSeverity::LOG) return "LOG";
}

// @see https://www.postgresql.org/docs/current/protocol-error-fields.html
enum class PostgresErrorField : unsigned char {
  SEVERITY_OLD = 'S',
  SEVERITY = 'V',  // Postgres 9.6 or later
  CODE = 'C',      // TODO(Matt): https://www.postgresql.org/docs/current/errcodes-appendix.html
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

class PostgresError {
 public:
  PostgresError(const network::PostgresSeverity severity,
                std::vector<std::pair<PostgresErrorField, std::string>> &&fields)
      : severity_(severity), fields_(std::move(fields)) {}

  PostgresError() = default;
  PostgresError &operator=(const PostgresError &other) = default;
  PostgresError &operator=(PostgresError &&other) noexcept = default;
  PostgresError(PostgresError &&other) noexcept = default;
  PostgresError(const PostgresError &other) = default;

  template <PostgresSeverity severity = PostgresSeverity::ERROR>
  static PostgresError Message(const std::string_view message) {
    return {severity,
            {{PostgresErrorField::SEVERITY, std::string(PostgresSeverityToString<severity>())},
             {PostgresErrorField::SEVERITY_OLD, std::string(PostgresSeverityToString<severity>())},
             {PostgresErrorField::HUMAN_READABLE_ERROR, std::string(message)}}};
  }

  // TODO(Matt): maybe needed eventually?
  //  template <PostgresSeverity severity = PostgresSeverity::ERROR>
  //  static PostgresError Message(std::string &&message) {
  //    return {severity,
  //            {{PostgresErrorField::SEVERITY, std::string(PostgresSeverityToString<severity>())},
  //             {PostgresErrorField::SEVERITY_OLD, std::string(PostgresSeverityToString<severity>())},
  //             {PostgresErrorField::HUMAN_READABLE_ERROR, std::move(message)}}};
  //  }

  void AddField(PostgresErrorField field, const std::string_view message) {
    fields_.emplace_back(field, std::string(message));
  }

  void AddField(PostgresErrorField field, std::string &&message) { fields_.emplace_back(field, std::move(message)); }

  PostgresSeverity GetSeverity() const { return severity_; }
  const std::vector<std::pair<PostgresErrorField, std::string>> &Fields() const { return fields_; }

 private:
  PostgresSeverity severity_ = PostgresSeverity::ERROR;
  std::vector<std::pair<PostgresErrorField, std::string>> fields_;
};

/**
 * Postgres Value Types
 * This defines all the types that we will support
 * We do not allow for user-defined types, nor do we try to do anything dynamic.
 * For more information, see 'pg_type.h' in Postgres
 * https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.h#L273
 */
enum class PostgresValueType : int32_t {
  INVALID = INVALID_TYPE_ID,
  BOOLEAN = 16,
  TINYINT = 16,  // BOOLEAN is an alias for TINYINT
  SMALLINT = 21,
  INTEGER = 23,
  VARBINARY = 17,
  BIGINT = 20,
  REAL = 700,
  DOUBLE = 701,
  TEXT = 25,
  BPCHAR = 1042,
  BPCHAR2 = 1014,
  VARCHAR = 1015,
  VARCHAR2 = 1043,
  DATE = 1082,
  TIMESTAMPS = 1114,
  TIMESTAMPS2 = 1184,
  TEXT_ARRAY = 1009,     // TEXTARRAYOID in postgres code
  INT2_ARRAY = 1005,     // INT2ARRAYOID in postgres code
  INT4_ARRAY = 1007,     // INT4ARRAYOID in postgres code
  OID_ARRAY = 1028,      // OIDARRAYOID in postgres code
  FLOADT4_ARRAY = 1021,  // FLOADT4ARRAYOID in postgres code
  DECIMAL = 1700
};

const uint32_t MAX_NAME_LENGTH = 63;  // Max length for internal name

}  // namespace terrier::network
