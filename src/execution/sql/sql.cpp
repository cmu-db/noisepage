#include "execution/sql/sql.h"

#include <string>

#include "common/error/exception.h"
#include "spdlog/fmt/fmt.h"
#include "type/type_id.h"

namespace noisepage::execution::sql {

// static
SqlTypeId GetSqlTypeFromInternalType(TypeId type) {
  switch (type) {
    case TypeId::Boolean:
      return SqlTypeId::Boolean;
    case TypeId::TinyInt:
      return SqlTypeId::TinyInt;
    case TypeId::SmallInt:
      return SqlTypeId::SmallInt;
    case TypeId::Integer:
      return SqlTypeId::Integer;
    case TypeId::BigInt:
      return SqlTypeId::BigInt;
    case TypeId::Float:
      return SqlTypeId::Real;
    case TypeId::Double:
      return SqlTypeId::Double;
    case TypeId::Date:
      return SqlTypeId::Date;
    case TypeId::Varchar:
      return SqlTypeId::Varchar;
    case TypeId::Varbinary:
      return SqlTypeId::Varchar;
    default:
      throw EXECUTION_EXCEPTION(fmt::format("Not a SQL type, {}.", TypeIdToString(type)),
                                common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
}

// static
std::size_t GetTypeIdSize(TypeId type) {
  switch (type) {
    case TypeId::Boolean:
      return sizeof(bool);
    case TypeId::TinyInt:
      return sizeof(int8_t);
    case TypeId::SmallInt:
      return sizeof(int16_t);
    case TypeId::Integer:
      return sizeof(int32_t);
    case TypeId::BigInt:
      return sizeof(int64_t);
    case TypeId::Hash:
      return sizeof(hash_t);
    case TypeId::Pointer:
      return sizeof(uintptr_t);
    case TypeId::Float:
      return sizeof(float);
    case TypeId::Double:
      return sizeof(double);
    case TypeId::Date:
      return sizeof(Date);
    case TypeId::Timestamp:
      return sizeof(Timestamp);
    case TypeId::Varchar:
      return sizeof(storage::VarlenEntry);
    case TypeId::Varbinary:
      return sizeof(Blob);
    default:
      // All cases handled
      UNREACHABLE("Impossible type");
  }
}

std::size_t GetTypeIdAlignment(TypeId type) {
  switch (type) {
    case TypeId::Boolean:
      return alignof(bool);
    case TypeId::TinyInt:
      return alignof(int8_t);
    case TypeId::SmallInt:
      return alignof(int16_t);
    case TypeId::Integer:
      return alignof(int32_t);
    case TypeId::BigInt:
      return alignof(int64_t);
    case TypeId::Hash:
      return alignof(hash_t);
    case TypeId::Pointer:
      return alignof(uintptr_t);
    case TypeId::Float:
      return alignof(float);
    case TypeId::Double:
      return alignof(double);
    case TypeId::Date:
      return alignof(Date);
    case TypeId::Varchar:
      return alignof(storage::VarlenEntry);
    case TypeId::Varbinary:
      return alignof(Blob);
    default:
      // All cases handled
      UNREACHABLE("Impossible type");
  }
}

// static
bool IsTypeFixedSize(TypeId type) {
  switch (type) {
    case TypeId::Boolean:
    case TypeId::TinyInt:
    case TypeId::SmallInt:
    case TypeId::Integer:
    case TypeId::BigInt:
    case TypeId::Hash:
    case TypeId::Pointer:
    case TypeId::Float:
    case TypeId::Double:
    case TypeId::Date:
      return true;
    case TypeId::Varchar:
    case TypeId::Varbinary:
      return false;
    default:
      UNREACHABLE("Impossible type");
  }
}

bool IsTypeIntegral(TypeId type) {
  switch (type) {
    case TypeId::TinyInt:
    case TypeId::SmallInt:
    case TypeId::Integer:
    case TypeId::BigInt:
      return true;
    case TypeId::Boolean:
    case TypeId::Hash:
    case TypeId::Pointer:
    case TypeId::Float:
    case TypeId::Double:
    case TypeId::Date:
    case TypeId::Varchar:
    case TypeId::Varbinary:
      return false;
    default:
      UNREACHABLE("Impossible type");
  }
}

bool IsTypeFloatingPoint(TypeId type) {
  switch (type) {
    case TypeId::Float:
    case TypeId::Double:
      return true;
    case TypeId::Boolean:
    case TypeId::TinyInt:
    case TypeId::SmallInt:
    case TypeId::Integer:
    case TypeId::BigInt:
    case TypeId::Hash:
    case TypeId::Pointer:
    case TypeId::Date:
    case TypeId::Varchar:
    case TypeId::Varbinary:
      return false;
    default:
      UNREACHABLE("Impossible type");
  }
}

// static
bool IsTypeNumeric(TypeId type) {
  switch (type) {
    case TypeId::Boolean:
    case TypeId::TinyInt:
    case TypeId::SmallInt:
    case TypeId::Integer:
    case TypeId::BigInt:
    case TypeId::Hash:
    case TypeId::Pointer:
    case TypeId::Float:
    case TypeId::Double:
    case TypeId::Date:
      return true;
    case TypeId::Varchar:
    case TypeId::Varbinary:
      return false;
    default:
      // All cases handled
      UNREACHABLE("Impossible type");
  }
}

// static
std::string TypeIdToString(TypeId type) {
  switch (type) {
    case TypeId::Boolean:
      return "Boolean";
    case TypeId::TinyInt:
      return "TinyInt";
    case TypeId::SmallInt:
      return "SmallInt";
    case TypeId::Integer:
      return "Integer";
    case TypeId::BigInt:
      return "BigInt";
    case TypeId::Hash:
      return "Hash";
    case TypeId::Pointer:
      return "Pointer";
    case TypeId::Float:
      return "Float";
    case TypeId::Double:
      return "Double";
    case TypeId::Date:
      return "Date";
    case TypeId::Timestamp:
      return "Timestamp";
    case TypeId::Varchar:
      return "VarChar";
    case TypeId::Varbinary:
      return "VarBinary";
    default:
      // All cases handled
      UNREACHABLE("Impossible type");
  }
}

TypeId GetTypeId(type::TypeId frontend_type) {
  execution::sql::TypeId execution_type_id;

  switch (frontend_type) {
    case type::TypeId::BOOLEAN:
      execution_type_id = execution::sql::TypeId::Boolean;
      break;
    case type::TypeId::TINYINT:
      execution_type_id = execution::sql::TypeId::TinyInt;
      break;
    case type::TypeId::SMALLINT:
      execution_type_id = execution::sql::TypeId::SmallInt;
      break;
    case type::TypeId::INTEGER:
      execution_type_id = execution::sql::TypeId::Integer;
      break;
    case type::TypeId::BIGINT:
      execution_type_id = execution::sql::TypeId::BigInt;
      break;
    case type::TypeId::DECIMAL:
      execution_type_id = execution::sql::TypeId::Double;
      break;
    case type::TypeId::TIMESTAMP:
      execution_type_id = execution::sql::TypeId::Timestamp;
      break;
    case type::TypeId::DATE:
      execution_type_id = execution::sql::TypeId::Date;
      break;
    case type::TypeId::VARCHAR:
      execution_type_id = execution::sql::TypeId::Varchar;
      break;
    case type::TypeId::VARBINARY:
      execution_type_id = execution::sql::TypeId::Varbinary;
      break;
    case type::TypeId::PARAMETER_OFFSET:
    case type::TypeId::INVALID:
    default:
      throw std::runtime_error("Cannot convert this frontend type to execution engine type.");
  }
  return execution_type_id;
}

}  // namespace noisepage::execution::sql
