#include "execution/sql/sql.h"

#include <string>

#include "common/error/error_code.h"
#include "common/error/exception.h"
#include "spdlog/fmt/fmt.h"
#include "storage/storage_defs.h"

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
uint16_t GetSqlTypeIdSize(SqlTypeId type) {
  switch (type) {
    case SqlTypeId::Boolean:
      return sizeof(bool);
    case SqlTypeId::TinyInt:
      return sizeof(int8_t);
    case SqlTypeId::SmallInt:
      return sizeof(int16_t);
    case SqlTypeId::Integer:
      return sizeof(int32_t);
    case SqlTypeId::BigInt:
      return sizeof(int64_t);
      //    case SqlTypeId::Float:    // TODO(Matt): not supported on front-end
      //      return sizeof(float);
    case SqlTypeId::Double:
      return sizeof(double);
    case SqlTypeId::Date:
      return sizeof(Date);
    case SqlTypeId::Timestamp:
      return sizeof(Timestamp);
    case SqlTypeId::Varchar:
    case SqlTypeId::Varbinary:
      return storage::VARLEN_COLUMN;
    case SqlTypeId::Decimal:
      return 16;  // TODO(Matt): double-check when fixed point decimal support merges
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
    case TypeId::Timestamp:
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
    case TypeId::Timestamp:
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

// static
std::string SqlTypeIdToString(SqlTypeId type) {
  switch (type) {
    case SqlTypeId::Boolean:
      return "Boolean";
    case SqlTypeId::TinyInt:
      return "TinyInt";
    case SqlTypeId::SmallInt:
      return "SmallInt";
    case SqlTypeId::Integer:
      return "Integer";
    case SqlTypeId::BigInt:
      return "BigInt";
    case SqlTypeId::Real:
      return "Real";
    case SqlTypeId::Double:
      return "Double";
    case SqlTypeId::Decimal:
      return "Decimal";
    case SqlTypeId::Date:
      return "Date";
    case SqlTypeId::Timestamp:
      return "Timestamp";
    case SqlTypeId::Varchar:
      return "Varchar";
    case SqlTypeId::Varbinary:
      return "Varbinary";
    default:
      // All cases handled
      UNREACHABLE("Impossible type");
  }
}

SqlTypeId SqlTypeIdFromString(const std::string &type_string) {
  if (type_string == "Invalid") {
    return SqlTypeId::Invalid;
  }
  if (type_string == "Boolean") {
    return SqlTypeId::Boolean;
  }
  if (type_string == "TinyInt") {
    return SqlTypeId::TinyInt;
  }
  if (type_string == "SmallInt") {
    return SqlTypeId::SmallInt;
  }
  if (type_string == "Integer") {
    return SqlTypeId::Integer;
  }
  if (type_string == "BigInt") {
    return SqlTypeId::BigInt;
  }
  if (type_string == "Double") {
    return SqlTypeId::Double;
  }
  if (type_string == "Decimal") {
    return SqlTypeId::Decimal;
  }
  if (type_string == "Timestamp") {
    return SqlTypeId::Timestamp;
  }
  if (type_string == "Date") {
    return SqlTypeId::Date;
  }
  if (type_string == "Varchar") {
    return SqlTypeId::Varchar;
  }
  if (type_string == "Varbinary") {
    return SqlTypeId::Varbinary;
  }
  UNREACHABLE(("No type conversion for string value " + type_string).c_str());
}

TypeId GetTypeId(SqlTypeId frontend_type) {
  TypeId execution_type_id;
  switch (frontend_type) {
    case SqlTypeId::Boolean:
      execution_type_id = execution::sql::TypeId::Boolean;
      break;
    case SqlTypeId::TinyInt:
      execution_type_id = execution::sql::TypeId::TinyInt;
      break;
    case SqlTypeId::SmallInt:
      execution_type_id = execution::sql::TypeId::SmallInt;
      break;
    case SqlTypeId::Integer:
      execution_type_id = execution::sql::TypeId::Integer;
      break;
    case SqlTypeId::BigInt:
      execution_type_id = execution::sql::TypeId::BigInt;
      break;
    case SqlTypeId::Double:
      execution_type_id = execution::sql::TypeId::Double;
      break;
    case SqlTypeId::Timestamp:
      execution_type_id = execution::sql::TypeId::Timestamp;
      break;
    case SqlTypeId::Date:
      execution_type_id = execution::sql::TypeId::Date;
      break;
    case SqlTypeId::Varchar:
      execution_type_id = execution::sql::TypeId::Varchar;
      break;
    case SqlTypeId::Varbinary:
      execution_type_id = execution::sql::TypeId::Varbinary;
      break;
    default:
      throw std::runtime_error("Cannot convert this frontend type to execution engine type.");
  }
  return execution_type_id;
}

}  // namespace noisepage::execution::sql
