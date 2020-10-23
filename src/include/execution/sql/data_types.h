#pragma once

#include <llvm/Support/Casting.h>

#include <string>
#include <type_traits>

#include "common/macros.h"
#include "execution/sql/sql.h"

namespace noisepage::execution::sql {

/**
 * Base class for algebraic SQL types.
 */
class SqlType {
 public:
  virtual ~SqlType() = default;

  /** @return ID of this SQL type. */
  SqlTypeId GetId() const { return id_; }

  /** @return True if this SQL type is nullable. */
  bool IsNullable() const { return nullable_; }

  /** @return Non-nullable version of this SQL type. */
  virtual const SqlType &GetNonNullableVersion() const = 0;

  /** @return Nullable version of this SQL type. */
  virtual const SqlType &GetNullableVersion() const = 0;

  /** @return Primitive type ID for this SQL type. */
  virtual TypeId GetPrimitiveTypeId() const = 0;

  /** @return Name of this SQL type. */
  virtual std::string GetName() const = 0;

  /** @return True if this SQL type is an integer type. */
  virtual bool IsIntegral() const = 0;

  /** @return True if this SQL type is a floating-point type. */
  virtual bool IsFloatingPoint() const = 0;

  /** @return True if this SQL type is a type that supports arithmetic. */
  virtual bool IsArithmetic() const = 0;

  /** @return True if this SQL type equals that SQL type. */
  virtual bool Equals(const SqlType &that) const = 0;

  /** @return True if this SQL type == that SQL type. */
  bool operator==(const SqlType &that) const noexcept { return Equals(that); }

  /** @return True if this SQL type != that SQL type. */
  bool operator!=(const SqlType &that) const noexcept { return !(*this == that); }

  // -------------------------------------------------------
  // Type-checking
  // -------------------------------------------------------

  /** @return True if this SQL type is the specified templated type. */
  template <typename T>
  bool Is() const {
    return llvm::isa<T>(this);
  }

  /** @return This SQL type casted to the specified templated type. */
  template <typename T>
  T *As() {
    return llvm::cast<T>(this);
  }

  /** @return This SQL type casted to the specified const templated type. */
  template <typename T>
  const T *As() const {
    return llvm::cast<const T>(this);
  }

  /** @return True if this SQL type can be casted to the specified templated type. */
  template <typename T>
  T *SafeAs() {
    return llvm::dyn_cast<T>(this);
  }

  /** @return True if this SQL type can be const casted to the specified templated type. */
  template <typename T>
  const T *SafeAs() const {
    return llvm::dyn_cast<const T>(this);
  }

 protected:
  /**
   * Construct the specified SQL type.
   * @param sql_type_id The type ID of the SQL type to be constructed.
   * @param nullable True if the specified SQL type is nullable, false otherwise.
   */
  explicit SqlType(SqlTypeId sql_type_id, bool nullable) : id_(sql_type_id), nullable_(nullable) {}

 private:
  /** The SQL type ID. */
  SqlTypeId id_;
  /** Flag indicating if the type is nullable. */
  bool nullable_;
};

/**
 * A SQL boolean type.
 */
class BooleanType : public SqlType {
 public:
  /** @return A non-nullable version of the boolean type. */
  static const BooleanType &InstanceNonNullable();

  /** @return A nullable version of the boolean type. */
  static const BooleanType &InstanceNullable();

  /** @return The boolean type with the requested nullability. */
  static const BooleanType &Instance(bool nullable) { return (nullable ? InstanceNullable() : InstanceNonNullable()); }

  const SqlType &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(); }

  TypeId GetPrimitiveTypeId() const override;

  std::string GetName() const override;

  bool IsIntegral() const override;

  bool IsFloatingPoint() const override;

  bool IsArithmetic() const override;

  bool Equals(const SqlType &that) const override;

  /** @return True if the input type is a Boolean type. */
  static bool classof(const SqlType *type) { return type->GetId() == SqlTypeId::Boolean; }  // NOLINT

 private:
  explicit BooleanType(bool nullable);
};

/**
 * Base for all types that are stored as primitive C/C++ numbers. This includes
 * the regular SQL numbers (smallint, int, decimal), but also dates and
 * timestamps.
 * @tparam CppType The primitive type
 */
template <typename CppType>
class NumberBaseType : public SqlType {
 public:
  /** @return Primitive type ID for this type. */
  TypeId GetPrimitiveTypeId() const override { return GetTypeId<CppType>(); }

  /** @return True if this type is an integer type. */
  bool IsIntegral() const override { return std::is_integral_v<CppType>; }

  /** @return True if this type is a floating-point type. */
  bool IsFloatingPoint() const override { return std::is_floating_point_v<CppType>; }

  /** @return True if this type is a type that supports arithmetic. */
  bool IsArithmetic() const override { return true; }

 protected:
  /** Construct the number type with the specified type ID and nullability. */
  NumberBaseType(SqlTypeId type_id, bool nullable) : SqlType(type_id, nullable) {}
};

/**
 * A SQL tiny-int type..
 */
class TinyIntType : public NumberBaseType<int8_t> {
 public:
  /** @return A non-nullable version of the tiny integer type. */
  static const TinyIntType &InstanceNonNullable();

  /** @return A nullable version of the tiny integer type. */
  static const TinyIntType &InstanceNullable();

  /** @return The tiny integer type with the requested nullability. */
  static const TinyIntType &Instance(bool nullable) { return (nullable ? InstanceNullable() : InstanceNonNullable()); }

  const SqlType &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(); }

  std::string GetName() const override;

  bool Equals(const SqlType &that) const override;

  /** @return True if the input type is a TinyInt type. */
  static bool classof(const SqlType *type) { return type->GetId() == SqlTypeId::TinyInt; }  // NOLINT

 private:
  explicit TinyIntType(bool nullable);
};

/**
 * A SQL small-int type..
 */
class SmallIntType : public NumberBaseType<int16_t> {
 public:
  /** @return A non-nullable version of the small integer type. */
  static const SmallIntType &InstanceNonNullable();

  /** @return A nullable version of the small integer type. */
  static const SmallIntType &InstanceNullable();

  /** @return The small integer type with the requested nullability. */
  static const SmallIntType &Instance(bool nullable) { return (nullable ? InstanceNullable() : InstanceNonNullable()); }

  const SqlType &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(); }

  std::string GetName() const override;

  bool Equals(const SqlType &that) const override;

  /** @return True if the input type is a SmallInt type. */
  static bool classof(const SqlType *type) { return type->GetId() == SqlTypeId::SmallInt; }  // NOLINT

 private:
  explicit SmallIntType(bool nullable);
};

/**
 * A SQL integer type.
 */
class IntegerType : public NumberBaseType<int32_t> {
 public:
  /** @return A non-nullable version of the integer type. */
  static const IntegerType &InstanceNonNullable();

  /** @return A nullable version of the integer type. */
  static const IntegerType &InstanceNullable();

  /** @return The integer type with the requested nullability. */
  static const IntegerType &Instance(bool nullable) { return (nullable ? InstanceNullable() : InstanceNonNullable()); }

  const SqlType &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(); }

  std::string GetName() const override;

  bool Equals(const SqlType &that) const override;

  /** @return True if the input type is a Integer type. */
  static bool classof(const SqlType *type) { return type->GetId() == SqlTypeId::Integer; }  // NOLINT

 private:
  explicit IntegerType(bool nullable);
};

/**
 * A SQL bigint type.
 */
class BigIntType : public NumberBaseType<int64_t> {
 public:
  /** @return A non-nullable version of the big integer type. */
  static const BigIntType &InstanceNonNullable();

  /** @return A nullable version of the big integer type. */
  static const BigIntType &InstanceNullable();

  /** @return The big integer type with the requested nullability. */
  static const BigIntType &Instance(bool nullable) { return (nullable ? InstanceNullable() : InstanceNonNullable()); }

  const SqlType &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(); }

  std::string GetName() const override;

  bool Equals(const SqlType &that) const override;

  /** @return True if the input type is a BigInt type. */
  static bool classof(const SqlType *type) { return type->GetId() == SqlTypeId::BigInt; }  // NOLINT

 private:
  explicit BigIntType(bool nullable);
};

/**
 * A SQL real type, i.e., a 4-byte floating point type.
 */
class RealType : public NumberBaseType<float> {
 public:
  /** @return A non-nullable version of the real type. */
  static const RealType &InstanceNonNullable();

  /** @return A nullable version of the real type. */
  static const RealType &InstanceNullable();

  /** @return The real type with the requested nullability. */
  static const RealType &Instance(bool nullable) { return (nullable ? InstanceNullable() : InstanceNonNullable()); }

  const SqlType &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(); }

  std::string GetName() const override;

  bool Equals(const SqlType &that) const override;

  /** @return True if the input type is a Real type. */
  static bool classof(const SqlType *type) { return type->GetId() == SqlTypeId::Real; }  // NOLINT

 private:
  explicit RealType(bool nullable);
};

/**
 * A SQL double type, i.e., an 8-byte floating point type.
 */
class DoubleType : public NumberBaseType<double> {
 public:
  /** @return A non-nullable version of the double type. */
  static const DoubleType &InstanceNonNullable();

  /** @return A nullable version of the double type. */
  static const DoubleType &InstanceNullable();

  /** @return The double type with the requested nullability. */
  static const DoubleType &Instance(bool nullable) { return (nullable ? InstanceNullable() : InstanceNonNullable()); }

  const SqlType &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(); }

  std::string GetName() const override;

  bool Equals(const SqlType &that) const override;

  /** @return True if the input type is a Double type. */
  static bool classof(const SqlType *type) { return type->GetId() == SqlTypeId::Double; }  // NOLINT

 private:
  explicit DoubleType(bool nullable);
};

/**
 * A SQL decimal type.
 */
class DecimalType : public SqlType {
 public:
  /** @return A non-nullable version of the decimal type. */
  static const DecimalType &InstanceNonNullable(uint32_t precision, uint32_t scale);

  /** @return A nullable version of the decimal type. */
  static const DecimalType &InstanceNullable(uint32_t precision, uint32_t scale);

  /** @return The decimal type with the requested nullability, precision, and scale. */
  static const DecimalType &Instance(bool nullable, uint32_t precision, uint32_t scale) {
    return (nullable ? InstanceNullable(precision, scale) : InstanceNonNullable(precision, scale));
  }

  const SqlType &GetNonNullableVersion() const override { return InstanceNonNullable(Precision(), Scale()); }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(Precision(), Scale()); }

  TypeId GetPrimitiveTypeId() const override;

  std::string GetName() const override;

  bool Equals(const SqlType &that) const override;

  bool IsIntegral() const override;

  bool IsFloatingPoint() const override;

  bool IsArithmetic() const override;

  /** @return The precision of this decimal type. */
  uint32_t Precision() const;

  /** @return The scale of this decimal type. */
  uint32_t Scale() const;

  /** @return True if the input type is a Decimal type. */
  static bool classof(const SqlType *type) { return type->GetId() == SqlTypeId::Decimal; }  // NOLINT

 private:
  DecimalType(bool nullable, uint32_t precision, uint32_t scale);

  template <bool nullable>
  static const DecimalType &InstanceInternal(uint32_t precision, uint32_t scale);

 private:
  uint32_t precision_;
  uint32_t scale_;
};

/**
 * A SQL date type.
 */
class DateType : public SqlType {
 public:
  /** @return A non-nullable version of the date type. */
  static const DateType &InstanceNonNullable();

  /** @return A nullable version of the date type. */
  static const DateType &InstanceNullable();

  /** @return The date type with the requested nullability. */
  static const DateType &Instance(bool nullable) { return (nullable ? InstanceNullable() : InstanceNonNullable()); }

  const SqlType &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(); }

  TypeId GetPrimitiveTypeId() const override;

  std::string GetName() const override;

  bool Equals(const SqlType &that) const override;

  bool IsIntegral() const override { return false; }

  bool IsFloatingPoint() const override { return false; }

  bool IsArithmetic() const override { return false; }

  /** @return True if the input type is a Date type. */
  static bool classof(const SqlType *type) { return type->GetId() == SqlTypeId::Date; }  // NOLINT

 private:
  explicit DateType(bool nullable);
};

/**
 * A SQL timestamp type.
 */
class TimestampType : public SqlType {
 public:
  /** @return A non-nullable version of the timestamp type. */
  static const TimestampType &InstanceNonNullable();

  /** @return A nullable version of the timestamp type. */
  static const TimestampType &InstanceNullable();

  /** @return The timestamp type with the requested nullability. */
  static const TimestampType &Instance(bool nullable) {
    return (nullable ? InstanceNullable() : InstanceNonNullable());
  }

  const SqlType &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(); }

  TypeId GetPrimitiveTypeId() const override;

  std::string GetName() const override;

  bool Equals(const SqlType &that) const override;

  bool IsIntegral() const override { return false; }

  bool IsFloatingPoint() const override { return false; }

  bool IsArithmetic() const override { return false; }

  /** @return True if the input type is a Timestamp type. */
  static bool classof(const SqlType *type) { return type->GetId() == SqlTypeId::Timestamp; }  // NOLINT

 private:
  explicit TimestampType(bool nullable);
};

/**
 * A SQL char type.
 */
class CharType : public SqlType {
 public:
  /** @return A non-nullable version of the char type. */
  static const CharType &InstanceNonNullable(uint32_t len);

  /** @return A nullable version of the char type. */
  static const CharType &InstanceNullable(uint32_t len);

  /** @return The char type with the requested nullability and length. */
  static const CharType &Instance(bool nullable, uint32_t len) {
    return (nullable ? InstanceNullable(len) : InstanceNonNullable(len));
  }

  const SqlType &GetNonNullableVersion() const override { return InstanceNonNullable(Length()); }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(Length()); }

  TypeId GetPrimitiveTypeId() const override;

  std::string GetName() const override;

  bool Equals(const SqlType &that) const override;

  bool IsIntegral() const override { return false; }

  bool IsFloatingPoint() const override { return false; }

  bool IsArithmetic() const override { return false; }

  /** @return The length of this char type. */
  uint32_t Length() const;

  /** @return True if the input type is a Char type. */
  static bool classof(const SqlType *type) { return type->GetId() == SqlTypeId::Char; }  // NOLINT

 private:
  explicit CharType(bool nullable, uint32_t length);

  template <bool nullable>
  static const CharType &InstanceInternal(uint32_t length);

 private:
  uint32_t length_;
};

/**
 * A SQL varchar type.
 */
class VarcharType : public SqlType {
 public:
  /** @return A non-nullable version of the varchar type. */
  static const VarcharType &InstanceNonNullable(uint32_t max_len);

  /** @return A nullable version of the varchar type. */
  static const VarcharType &InstanceNullable(uint32_t max_len);

  /** @return The varchar type with the requested nullability. */
  static const VarcharType &Instance(bool nullable, uint32_t max_len) {
    return (nullable ? InstanceNullable(max_len) : InstanceNonNullable(max_len));
  }

  const SqlType &GetNonNullableVersion() const override { return InstanceNonNullable(MaxLength()); }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(MaxLength()); }

  TypeId GetPrimitiveTypeId() const override;

  std::string GetName() const override;

  bool Equals(const SqlType &that) const override;

  bool IsIntegral() const override { return false; }

  bool IsFloatingPoint() const override { return false; }

  bool IsArithmetic() const override { return false; }

  /** @return The maximum length of this varchar type. */
  uint32_t MaxLength() const;

  /** @return True if the input type is a Varchar type. */
  static bool classof(const SqlType *type) { return type->GetId() == SqlTypeId::Varchar; }  // NOLINT

 private:
  explicit VarcharType(bool nullable, uint32_t max_len);

  template <bool nullable>
  static const VarcharType &InstanceInternal(uint32_t length);

 private:
  uint32_t max_len_;
};

}  // namespace noisepage::execution::sql
