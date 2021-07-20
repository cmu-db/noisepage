#pragma once

#include <cstring>
#include <string>

#include "common/macros.h"
#include "execution/sql/runtime_types.h"

namespace noisepage::execution::sql {

/**
 * All internal types underlying the SQL types. This is a superset of the SQL types meant to capture
 * the fact that vectors can contain non-SQL data such as hash values and pointers, created during
 * query execution.
 */
enum class TypeId : uint8_t {
  Boolean,    // bool
  TinyInt,    // int8_t
  SmallInt,   // int16_t
  Integer,    // int32_t
  BigInt,     // int64_t
  Hash,       // hash_t
  Pointer,    // uintptr_t
  Float,      // float
  Double,     // double
  Date,       // Date objects
  Timestamp,  // Timestamp objects
  Varchar,    // char*, representing a null-terminated UTF-8 string
  Varbinary   // blobs representing arbitrary bytes
};

/**
 * Supported SQL data types. //TODO(Matt): merge with network::PostgresValueType to reduce translation?
 */
enum class SqlTypeId : int8_t {
  Invalid = -1,  // TODO(Matt): this is a hack, mostly used to transport un-typed NULLs after parsing
  Boolean,
  TinyInt,    // 1-byte integer
  SmallInt,   // 2-byte integer
  Integer,    // 4-byte integer
  BigInt,     // 8-byte integer
  Real,       // 4-byte float //TODO(Matt): front-end doesn't support this, just changes REAL to DOUBLE
  Double,     // 8-byte float
  Decimal,    // Arbitrary-precision numeric //TODO(Matt): back-end doesn't support this. See #1434
  Date,       // Dates
  Timestamp,  // Timestamps
  Char,       // Fixed-length string //TODO(Matt): front-end doesn't support this
  Varchar,    // Variable-length string
  Varbinary,  // TODO(Matt): front-end doesn't support this. See #788
  Variadic    // TODO(Matt): hack from type::TypeId having a VARIADIC. See #1183
};

/**
 * The possible column compression/encodings.
 */
enum class ColumnEncoding : uint8_t {
  None,
  Rle,
  Delta,
  IntegerDict,
  StringDict,
};

/**
 * All possible JOIN types.
 */
enum class JoinType : uint8_t { Inner, Outer, Left, Right, Anti, Semi };

/**
 * @return The simplest SQL type for primitive type ID @em type.
 */
SqlTypeId GetSqlTypeFromInternalType(TypeId type);

/**
 * @return The execution type ID corresponding to the given frontend type.
 */
TypeId GetTypeId(SqlTypeId frontend_type);

/**
 * @return The primitive type ID for the C/C++ template type @em T.
 */
template <class T>
constexpr inline TypeId GetTypeId() {
  if constexpr (std::is_same<T, bool>()) {
    return TypeId::Boolean;
  } else if constexpr (std::is_same<std::remove_const_t<T>, int8_t>()) {  // NOLINT
    return TypeId::TinyInt;
  } else if constexpr (std::is_same<std::remove_const_t<T>, int16_t>()) {  // NOLINT
    return TypeId::SmallInt;
  } else if constexpr (std::is_same<std::remove_const_t<T>, int32_t>()) {  // NOLINT
    return TypeId::Integer;
  } else if constexpr (std::is_same<std::remove_const_t<T>, int64_t>()) {  // NOLINT
    return TypeId::BigInt;
  } else if constexpr (std::is_same<std::remove_const_t<T>, hash_t>()) {  // NOLINT
    return TypeId::Hash;
  } else if constexpr (std::is_same<std::remove_const_t<T>, uintptr_t>()) {  // NOLINT
    return TypeId::Pointer;
  } else if constexpr (std::is_same<std::remove_const_t<T>, float>()) {  // NOLINT
    return TypeId::Float;
  } else if constexpr (std::is_same<std::remove_const_t<T>, double>()) {  // NOLINT
    return TypeId::Double;
  } else if constexpr (std::is_same<std::remove_const_t<T>, Date>()) {  // NOLINT
    return TypeId::Date;
  } else if constexpr (std::is_same<std::remove_const_t<T>, Timestamp>()) {  // NOLINT
    return TypeId::Timestamp;
  } else if constexpr (std::is_same<std::remove_const_t<T>, char *>() ||  // NOLINT
                       std::is_same<std::remove_const_t<T>, const char *>() ||
                       std::is_same<std::remove_const_t<T>, std::string>() ||
                       std::is_same<std::remove_const_t<T>, std::string_view>() ||
                       std::is_same<std::remove_const_t<T>, storage::VarlenEntry>()) {
    return TypeId::Varchar;
  } else if constexpr (std::is_same<std::remove_const_t<T>, Blob>()) {  // NOLINT
    return TypeId::Varbinary;
  }
  static_assert("Not a valid primitive type");
}

/**
 * @return The size in bytes of a value with the primitive type @em type.
 */
std::size_t GetTypeIdSize(TypeId type);

/**
 * @return The size in bytes of a value with the primitive type @em type.
 */
uint16_t GetSqlTypeIdSize(SqlTypeId type);

/**
 * @return The alignment in bytes of a value with the primitive type @em type.
 */
std::size_t GetTypeIdAlignment(TypeId type);

SqlTypeId SqlTypeIdFromString(const std::string &type_string);

std::string SqlTypeIdToString(SqlTypeId type);

/**
 * @return True if the primitive type ID @em type is a fixed-size type; false otherwise.
 */
bool IsTypeFixedSize(TypeId type);

/**
 * @return True if the provided primitive type is an integral type.
 */
bool IsTypeIntegral(TypeId type);

/**
 * @return True if the provided primitive type is a floating point type.
 */
bool IsTypeFloatingPoint(TypeId type);

/**
 * @return True if the primitive type ID @em type is a numeric type; false otherwise.
 */
bool IsTypeNumeric(TypeId type);

/**
 * @return A string representation of the input type ID @em type.
 */
std::string TypeIdToString(TypeId type);

}  // namespace noisepage::execution::sql
