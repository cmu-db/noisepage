#pragma once

#include <iosfwd>
#include <string>
#include <string_view>

#include "execution/sql/data_types.h"
#include "execution/sql/runtime_types.h"

namespace noisepage::execution::exec {
class ExecutionSettings;
}  // namespace noisepage::execution::exec

namespace noisepage::execution::sql {

struct Val;

/**
 * A generic value is a glorified typed-union representing some primitive value.
 * This is purely a container class and isn't used for actual expression
 * evaluation or performance-critical execution.
 */
class EXPORT GenericValue {
  friend class Vector;
  friend class VectorOps;
  friend class GenericValueTests;

 public:
  /**
   * @return The SQL type of this value.
   */
  TypeId GetTypeId() const noexcept { return type_id_; }

  /**
   * @return If this value is NULL.
   */
  bool IsNull() const noexcept { return is_null_; }

  /**
   * Is this value equal to the provided value? This is NOT SQL equivalence!
   * @param other The value to compare with.
   * @return True if equal; false otherwise.
   */
  bool Equals(const GenericValue &other) const;

  /**
   * Cast this value to the given type.
   * @param exec_settings The execution settings.
   * @param type The type to cast to.
   */
  GenericValue CastTo(const exec::ExecutionSettings &exec_settings, TypeId type);

  /**
   * Copy this value.
   */
  GenericValue Copy() const { return GenericValue(*this); }

  /**
   * Convert this generic value into a string.
   * @return The string representation of this value.
   */
  std::string ToString() const;

  /**
   * @return True if this value is equal @em that. Note that this is NOT SQL equality!
   */
  bool operator==(const GenericValue &that) const { return Equals(that); }

  /**
   * @return True if this value is not equal to @em that. Note that this is NOT SQL inequality!
   */
  bool operator!=(const GenericValue &that) const { return !(*this == that); }

  // -------------------------------------------------------
  // Static factory methods
  // -------------------------------------------------------

  /**
   * Create a NULL value.
   * @param type_id The type of the value.
   * @return A NULL value.
   */
  static GenericValue CreateNull(TypeId type_id);

  /**
   * Create a non-NULL boolean value.
   * @param value The value.
   * @return A Boolean value.
   */
  static GenericValue CreateBoolean(bool value);

  /**
   * Create a non-NULL tinyint value.
   * @param value The value.
   * @return A TinyInt value.
   */
  static GenericValue CreateTinyInt(int8_t value);

  /**
   * Create a non-NULL smallint value.
   * @param value The value.
   * @return A SmallInt value.
   */
  static GenericValue CreateSmallInt(int16_t value);

  /**
   * Create a non-NULL integer value.
   * @param value The value.
   * @return An Integer value.
   */
  static GenericValue CreateInteger(int32_t value);

  /**
   * Create a non-NULL bigint value.
   * @param value The value.
   * @return A BigInt value.
   */
  static GenericValue CreateBigInt(int64_t value);

  /**
   * Create a non-NULL hash value.
   * @param value The value.
   * @return A hash value.
   */
  static GenericValue CreateHash(hash_t value);

  /**
   * Create a non-NULL pointer value.
   * @param value The value.
   * @return A pointer value.
   */
  static GenericValue CreatePointer(uintptr_t value);

  /**
   * Create a non-NULL pointer value.
   * @param pointer The pointer value.
   * @return A pointer value.
   */
  template <typename T>
  static GenericValue CreatePointer(T *pointer) {
    return CreatePointer(reinterpret_cast<uintptr_t>(pointer));
  }

  /**
   * Create a non-NULL real value.
   * @param value The value.
   * @return A Real value.
   */
  static GenericValue CreateReal(float value);

  /**
   * Create a non-NULL float value.
   * @param value The value.
   * @return A float value.
   */
  static GenericValue CreateFloat(float value) { return CreateReal(value); }

  /**
   * Create a non-NULL double value.
   * @param value The value.
   * @return A Double value.
   */
  static GenericValue CreateDouble(double value);

  /**
   * Create a non-NULL date value.
   * @param date The date.
   * @return A date value.
   */
  static GenericValue CreateDate(Date date);

  /**
   * Create a non-NULL date value.
   * @param year The year of the date.
   * @param month The month of the date.
   * @param day The day of the date.
   * @return A Date value.
   */
  static GenericValue CreateDate(uint32_t year, uint32_t month, uint32_t day);

  /**
   * Crete a non-NULL timestamp value.
   * @param timestamp The timestamp.
   * @return A timestamp value.
   */
  static GenericValue CreateTimestamp(Timestamp timestamp);

  /**
   * Create a non-NULL timestamp value.
   * @param year The year of the timestamp.
   * @param month The month of the timestamp.
   * @param day The day of the timestamp.
   * @param hour The hour of the timestamp.
   * @param min The minute of the timestamp.
   * @param sec The second of the timestamp.
   * @return A Timestamp value.
   */
  static GenericValue CreateTimestamp(int32_t year, int32_t month, int32_t day, int32_t hour, int32_t min, int32_t sec);

  /**
   * Create a non-NULL varchar value.
   * @param str The string view.
   * @return A Varchar value.
   */
  static GenericValue CreateVarchar(std::string_view str);

  /**
   * Create a generic value from a runtime value.
   * @param type_id The type of the runtime value.
   * @param val The true SQL value.
   * @return The generic value equivalent to the provided explicit runtime value.
   */
  static GenericValue CreateFromRuntimeValue(TypeId type_id, const Val &val);

  /** Output to stream. */
  friend std::ostream &operator<<(std::ostream &out, const GenericValue &val);

 private:
  /** Private constructor to force usage of factory methods. */
  explicit GenericValue(TypeId type_id) : type_id_(type_id), is_null_(true), value_() {}

 private:
  /** The primitive type */
  TypeId type_id_;
  /** Is this value null? */
  bool is_null_;
  /** The value of the object if it's a fixed-length type */
  union {
    bool boolean_;
    int8_t tinyint_;
    int16_t smallint_;
    int32_t integer_;
    int64_t bigint_;
    hash_t hash_;
    uintptr_t pointer_;
    Date date_;
    Timestamp timestamp_;
    float float_;
    double double_;
  } value_;
  // The value of the object if it's a variable size type.
  std::string str_value_;
};

}  // namespace noisepage::execution::sql
