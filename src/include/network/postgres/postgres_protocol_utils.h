#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "network/network_defs.h"
#include "network/network_io_utils.h"
#include "network/postgres/postgres_defs.h"
#include "type/transient_value_peeker.h"

namespace terrier::network {

// TODO(Tianyu): It looks very broken that this never changes.
// clang-format off

 /**
  * Hardcoded server parameter values to send to the client
  */
  const std::unordered_map<std::string, std::string>
    PG_PARAMETER_STATUS_MAP = {
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

type::TypeId PostgresValueTypeToInternalValueType(PostgresValueType type);
PostgresValueType InternalValueTypeToPostgresValueType(type::TypeId type);

}  // namespace terrier::network
