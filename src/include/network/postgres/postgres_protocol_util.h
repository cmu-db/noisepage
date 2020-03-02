#pragma once

#include <string>
#include <utility>

#include "common/exception.h"
#include "network/network_defs.h"
#include "network/postgres/postgres_defs.h"

namespace terrier::network {

/**
 * Utility class for dealing with the Postgres network protocol
 */
class PostgresProtocolUtil {
 public:
  PostgresProtocolUtil() = delete;

  /**
   * Convert a PostgresValueType to our internal TypeId.
   * This will throw an exception if you give it a type that we cannot convert.
   * @param type the input type
   * @return output type
   */
  static type::TypeId PostgresValueTypeToInternalValueType(PostgresValueType type);

  /**
   * Convert our internal TypeId to a PostgresValueType.
   * This will throw an exception if you give it a type that we cannot convert.
   * @param type the input type
   * @return output type
   */
  static PostgresValueType InternalValueTypeToPostgresValueType(type::TypeId type);
};

}  // namespace terrier::network
