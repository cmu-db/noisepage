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
