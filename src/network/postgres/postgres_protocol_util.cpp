#include "network/postgres/postgres_protocol_util.h"

#include <sstream>

#include "loggers/network_logger.h"

namespace noisepage::network {

execution::sql::SqlTypeId PostgresProtocolUtil::PostgresValueTypeToInternalValueType(const PostgresValueType type) {
  switch (type) {
    case PostgresValueType::INVALID:
      return execution::sql::SqlTypeId::Invalid;

    case PostgresValueType::BOOLEAN:
      return execution::sql::SqlTypeId::Boolean;

    case PostgresValueType::SMALLINT:
      return execution::sql::SqlTypeId::SmallInt;
    case PostgresValueType::INTEGER:
      return execution::sql::SqlTypeId::Integer;
    case PostgresValueType::BIGINT:
      return execution::sql::SqlTypeId::BigInt;
    case PostgresValueType::REAL:
    case PostgresValueType::DOUBLE:
      return execution::sql::SqlTypeId::Double;

    case PostgresValueType::BPCHAR:
    case PostgresValueType::BPCHAR2:
    case PostgresValueType::VARCHAR:
    case PostgresValueType::VARCHAR2:
    case PostgresValueType::TEXT:
      return execution::sql::SqlTypeId::Varchar;

    case PostgresValueType::VARBINARY:
      return execution::sql::SqlTypeId::Varbinary;

    case PostgresValueType::DATE:
      return execution::sql::SqlTypeId::Date;
    case PostgresValueType::TIMESTAMPS:
    case PostgresValueType::TIMESTAMPS2:
      return execution::sql::SqlTypeId::Timestamp;

    case PostgresValueType::DECIMAL:
      return execution::sql::SqlTypeId::Decimal;
    default: {
      std::ostringstream os;
      os << "No TypeId conversion for PostgresValueType '" << static_cast<int>(type) << "'";
      throw NETWORK_PROCESS_EXCEPTION(os.str().c_str());
    }
  }
}

PostgresValueType PostgresProtocolUtil::InternalValueTypeToPostgresValueType(const execution::sql::SqlTypeId type) {
  switch (type) {
    case execution::sql::SqlTypeId::Invalid:
      return PostgresValueType::INVALID;

    case execution::sql::SqlTypeId::Boolean:
      return PostgresValueType::BOOLEAN;

    case execution::sql::SqlTypeId::TinyInt:
      return PostgresValueType::TINYINT;

    case execution::sql::SqlTypeId::SmallInt:
      return PostgresValueType::SMALLINT;

    case execution::sql::SqlTypeId::Integer:
      return PostgresValueType::INTEGER;

    case execution::sql::SqlTypeId::BigInt:
      return PostgresValueType::BIGINT;

    case execution::sql::SqlTypeId::Double:
      return PostgresValueType::DOUBLE;

    case execution::sql::SqlTypeId::Decimal:
      return PostgresValueType::DECIMAL;

    case execution::sql::SqlTypeId::Timestamp:
      return PostgresValueType::TIMESTAMPS;

    case execution::sql::SqlTypeId::Date:
      return PostgresValueType::DATE;

    case execution::sql::SqlTypeId::Varchar:
      return PostgresValueType::VARCHAR2;  // TODO(Matt): should this just be VARCHAR? VARCHAR2 is an Postgres-Oracle
                                           // compatibility thing

    case execution::sql::SqlTypeId::Varbinary:
      return PostgresValueType::VARBINARY;

    default: {
      std::ostringstream os;
      os << "No PostgresValueType conversion for TypeId '" << static_cast<int>(type) << "'";
      throw NETWORK_PROCESS_EXCEPTION(os.str().c_str());
    }
  }
}

}  // namespace noisepage::network
