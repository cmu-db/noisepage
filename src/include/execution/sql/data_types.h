#pragma once

#include <string>
#include <type_traits>

#include "common/macros.h"
#include "execution/sql/sql.h"
#include "llvm/Support/Casting.h"

namespace terrier::execution::sql {

/**
 * Base class for algebraic SQL types.
 */
class SqlType {
 public:
  virtual ~SqlType() = default;

  SqlTypeId GetId() const { return id_; }

  bool IsNullable() const { return nullable_; }

  virtual const SqlType &GetNonNullableVersion() const = 0;

  virtual const SqlType &GetNullableVersion() const = 0;

  virtual TypeId GetPrimitiveTypeId() const = 0;

  virtual std::string GetName() const = 0;

  virtual bool IsIntegral() const = 0;

  virtual bool IsFloatingPoint() const = 0;

  virtual bool IsArithmetic() const = 0;

  virtual bool Equals(const SqlType &that) const = 0;

  bool operator==(const SqlType &that) const noexcept { return Equals(that); }

  bool operator!=(const SqlType &that) const noexcept { return !(*this == that); }

  // -------------------------------------------------------
  // Type-checking
  // -------------------------------------------------------

  template <typename T>
  bool Is() const {
    return llvm::isa<T>(this);
  }

  template <typename T>
  T *As() {
    return llvm::cast<T>(this);
  }

  template <typename T>
  const T *As() const {
    return llvm::cast<const T>(this);
  }

  template <typename T>
  T *SafeAs() {
    return llvm::dyn_cast<T>(this);
  }

  template <typename T>
  const T *SafeAs() const {
    return llvm::dyn_cast<const T>(this);
  }

 protected:
  explicit SqlType(SqlTypeId sql_type_id, bool nullable) : id_(sql_type_id), nullable_(nullable) {}

 private:
  // The SQL type ID
  SqlTypeId id_;
  // Flag indicating if the type is nullable
  bool nullable_;
};

/**
 * A SQL boolean type.
 */
class BooleanType : public SqlType {
 public:
  static const BooleanType &InstanceNonNullable();

  static const BooleanType &InstanceNullable();

  static const BooleanType &Instance(bool nullable) { return (nullable ? InstanceNullable() : InstanceNonNullable()); }

  const SqlType &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(); }

  TypeId GetPrimitiveTypeId() const override;

  std::string GetName() const override;

  bool IsIntegral() const override;

  bool IsFloatingPoint() const override;

  bool IsArithmetic() const override;

  bool Equals(const SqlType &that) const override;

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
  TypeId GetPrimitiveTypeId() const override { return GetTypeId<CppType>(); }

  bool IsIntegral() const override { return std::is_integral_v<CppType>; }

  bool IsFloatingPoint() const override { return std::is_floating_point_v<CppType>; }

  bool IsArithmetic() const override { return true; }

 protected:
  NumberBaseType(SqlTypeId type_id, bool nullable) : SqlType(type_id, nullable) {}
};

/**
 * A SQL tiny-int type..
 */
class TinyIntType : public NumberBaseType<int8_t> {
 public:
  static const TinyIntType &InstanceNonNullable();

  static const TinyIntType &InstanceNullable();

  static const TinyIntType &Instance(bool nullable) { return (nullable ? InstanceNullable() : InstanceNonNullable()); }

  const SqlType &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(); }

  std::string GetName() const override;

  bool Equals(const SqlType &that) const override;

  static bool classof(const SqlType *type) { return type->GetId() == SqlTypeId::TinyInt; }  // NOLINT

 private:
  explicit TinyIntType(bool nullable);
};

/**
 * A SQL small-int type..
 */
class SmallIntType : public NumberBaseType<int16_t> {
 public:
  static const SmallIntType &InstanceNonNullable();

  static const SmallIntType &InstanceNullable();

  static const SmallIntType &Instance(bool nullable) { return (nullable ? InstanceNullable() : InstanceNonNullable()); }

  const SqlType &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(); }

  std::string GetName() const override;

  bool Equals(const SqlType &that) const override;

  static bool classof(const SqlType *type) { return type->GetId() == SqlTypeId::SmallInt; }  // NOLINT

 private:
  explicit SmallIntType(bool nullable);
};

/**
 * A SQL integer type.
 */
class IntegerType : public NumberBaseType<int32_t> {
 public:
  static const IntegerType &InstanceNonNullable();

  static const IntegerType &InstanceNullable();

  static const IntegerType &Instance(bool nullable) { return (nullable ? InstanceNullable() : InstanceNonNullable()); }

  const SqlType &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(); }

  std::string GetName() const override;

  bool Equals(const SqlType &that) const override;

  static bool classof(const SqlType *type) { return type->GetId() == SqlTypeId::Integer; }  // NOLINT

 private:
  explicit IntegerType(bool nullable);
};

/**
 * A SQL bigint type.
 */
class BigIntType : public NumberBaseType<int64_t> {
 public:
  static const BigIntType &InstanceNonNullable();

  static const BigIntType &InstanceNullable();

  static const BigIntType &Instance(bool nullable) { return (nullable ? InstanceNullable() : InstanceNonNullable()); }

  const SqlType &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(); }

  std::string GetName() const override;

  bool Equals(const SqlType &that) const override;

  static bool classof(const SqlType *type) { return type->GetId() == SqlTypeId::BigInt; }  // NOLINT

 private:
  explicit BigIntType(bool nullable);
};

/**
 * A SQL real type, i.e., a 4-byte floating point type.
 */
class RealType : public NumberBaseType<float> {
 public:
  static const RealType &InstanceNonNullable();

  static const RealType &InstanceNullable();

  static const RealType &Instance(bool nullable) { return (nullable ? InstanceNullable() : InstanceNonNullable()); }

  const SqlType &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(); }

  std::string GetName() const override;

  bool Equals(const SqlType &that) const override;

  static bool classof(const SqlType *type) { return type->GetId() == SqlTypeId::Real; }  // NOLINT

 private:
  explicit RealType(bool nullable);
};

/**
 * A SQL double type, i.e., an 8-byte floating point type.
 */
class DoubleType : public NumberBaseType<double> {
 public:
  static const DoubleType &InstanceNonNullable();

  static const DoubleType &InstanceNullable();

  static const DoubleType &Instance(bool nullable) { return (nullable ? InstanceNullable() : InstanceNonNullable()); }

  const SqlType &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(); }

  std::string GetName() const override;

  bool Equals(const SqlType &that) const override;

  static bool classof(const SqlType *type) { return type->GetId() == SqlTypeId::Double; }  // NOLINT

 private:
  explicit DoubleType(bool nullable);
};

/**
 * A SQL decimal type.
 */
class DecimalType : public SqlType {
 public:
  static const DecimalType &InstanceNonNullable(uint32_t precision, uint32_t scale);

  static const DecimalType &InstanceNullable(uint32_t precision, uint32_t scale);

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

  uint32_t Precision() const;

  uint32_t Scale() const;

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
  static const DateType &InstanceNonNullable();

  static const DateType &InstanceNullable();

  static const DateType &Instance(bool nullable) { return (nullable ? InstanceNullable() : InstanceNonNullable()); }

  const SqlType &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const SqlType &GetNullableVersion() const override { return InstanceNullable(); }

  TypeId GetPrimitiveTypeId() const override;

  std::string GetName() const override;

  bool Equals(const SqlType &that) const override;

  bool IsIntegral() const override { return false; }

  bool IsFloatingPoint() const override { return false; }

  bool IsArithmetic() const override { return false; }

  static bool classof(const SqlType *type) { return type->GetId() == SqlTypeId::Date; }  // NOLINT

 private:
  explicit DateType(bool nullable);
};

/**
 * A SQL timestamp type.
 */
class TimestampType : public SqlType {
 public:
  static const TimestampType &InstanceNonNullable();

  static const TimestampType &InstanceNullable();

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

  static bool classof(const SqlType *type) { return type->GetId() == SqlTypeId::Date; }  // NOLINT

 private:
  explicit TimestampType(bool nullable);
};

/**
 * A SQL char type.
 */
class CharType : public SqlType {
 public:
  static const CharType &InstanceNonNullable(uint32_t len);

  static const CharType &InstanceNullable(uint32_t len);

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

  uint32_t Length() const;

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
  static const VarcharType &InstanceNonNullable(uint32_t max_len);

  static const VarcharType &InstanceNullable(uint32_t max_len);

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

  uint32_t MaxLength() const;

  static bool classof(const SqlType *type) { return type->GetId() == SqlTypeId::Varchar; }  // NOLINT

 private:
  explicit VarcharType(bool nullable, uint32_t max_len);

  template <bool nullable>
  static const VarcharType &InstanceInternal(uint32_t length);

 private:
  uint32_t max_len_;
};

}  // namespace terrier::execution::sql
