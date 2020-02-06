#include "network/postgres/postgres_protocol_utils.h"

#include "loggers/network_logger.h"

namespace terrier::network {

type::TypeId PostgresValueTypeToInternalValueType(const PostgresValueType type) {
  switch (type) {
    case PostgresValueType::BOOLEAN:
      return type::TypeId::BOOLEAN;

    case PostgresValueType::SMALLINT:
      return type::TypeId::SMALLINT;
    case PostgresValueType::INTEGER:
      return type::TypeId::INTEGER;
    case PostgresValueType::BIGINT:
      return type::TypeId::BIGINT;
    case PostgresValueType::REAL:
      return type::TypeId::DECIMAL;
    case PostgresValueType::DOUBLE:
      return type::TypeId::DECIMAL;

    case PostgresValueType::BPCHAR:
    case PostgresValueType::BPCHAR2:
    case PostgresValueType::VARCHAR:
    case PostgresValueType::VARCHAR2:
    case PostgresValueType::TEXT:
      return type::TypeId::VARCHAR;

    case PostgresValueType::DATE:
    case PostgresValueType::TIMESTAMPS:
    case PostgresValueType::TIMESTAMPS2:
      return type::TypeId::TIMESTAMP;

    case PostgresValueType::DECIMAL:
      return type::TypeId::DECIMAL;
    default:
      NETWORK_LOG_ERROR(fmt::format("No TypeId conversion for PostgresValueType value '%d'", static_cast<int>(type)));
      throw NETWORK_PROCESS_EXCEPTION("");
  }
}

PostgresValueType InternalValueTypeToPostgresValueType(const type::TypeId type) {
  switch (type) {
    case type::TypeId::INVALID:
      return PostgresValueType::INVALID;

    case type::TypeId::BOOLEAN:
      return PostgresValueType::BOOLEAN;

    case type::TypeId::TINYINT:
      return PostgresValueType::TINYINT;

    case type::TypeId::SMALLINT:
      return PostgresValueType::SMALLINT;

    case type::TypeId::INTEGER:
      return PostgresValueType::INTEGER;

    case type::TypeId::BIGINT:
      return PostgresValueType::BIGINT;

    case type::TypeId::DECIMAL:
      return PostgresValueType::DOUBLE;

    case type::TypeId::TIMESTAMP:
      return PostgresValueType::TIMESTAMPS;

    case type::TypeId::DATE:
      return PostgresValueType::DATE;

    case type::TypeId::VARCHAR:
      return PostgresValueType::VARCHAR2;

    case type::TypeId::VARBINARY:
      return PostgresValueType::VARBINARY;

    default:
      NETWORK_LOG_ERROR(fmt::format("No TypeId conversion for PostgresValueType value '%d'", static_cast<int>(type)));
      throw NETWORK_PROCESS_EXCEPTION("");
  }
}

}  // namespace terrier::network
