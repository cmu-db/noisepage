#include "network/postgres_protocol_utils.h"

namespace terrier::network {

type::TypeId PostgresValueTypeToInternalValueType(PostgresValueType type) {
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
      LOG_ERROR(fmt::format("No TypeId conversion for PostgresValueType value '%d'", static_cast<int>(type)));
      throw NETWORK_PROCESS_EXCEPTION("");
  }
}

}
