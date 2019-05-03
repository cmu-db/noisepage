#pragma once

#include <string>

#include "llvm/Support/Casting.h"

#include "execution/util/common.h"
#include "execution/util/macros.h"

namespace tpl::sql {

/**
 * Supported SQL data types
 */
enum class TypeId : u8 { Boolean, SmallInt, Integer, BigInt, Decimal, Date, Char, Varchar };

/// Base algebraic SQL type
class Type {
 public:
  virtual ~Type() = default;

  TypeId type_id() const { return type_id_; }

  bool nullable() const { return nullable_; }

  virtual const Type &GetNonNullableVersion() const = 0;

  virtual const Type &GetNullableVersion() const = 0;

  virtual std::string GetName() const = 0;

  virtual bool IsArithmetic() const = 0;

  virtual bool Equals(const Type &other) const = 0;

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
  explicit Type(TypeId type_id, bool nullable) : type_id_(type_id), nullable_(nullable) {}

 private:
  TypeId type_id_;

  bool nullable_;
};

class BooleanType : public Type {
 public:
  static const BooleanType &InstanceNonNullable();

  static const BooleanType &InstanceNullable();

  static const BooleanType &Instance(bool nullable) { return (nullable ? InstanceNullable() : InstanceNonNullable()); }

  const Type &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const Type &GetNullableVersion() const override { return InstanceNullable(); }

  std::string GetName() const override;

  bool IsArithmetic() const override;

  bool Equals(const Type &other) const override;

  static bool classof(const Type *type) { return type->type_id() == TypeId::Boolean; }

 private:
  explicit BooleanType(bool nullable);
};

/// Base for all types that are stored as primitive C/C++ numbers. This includes
/// the regular SQL numbers (smallint, int, decimal), but also dates and
/// timestamps.
///
/// \tparam CppType The primitive type
template <typename CppType>
class NumberBaseType : public Type {
 public:
  using PrimitiveType = CppType;

  bool IsArithmetic() const override { return true; }

 protected:
  NumberBaseType(TypeId type_id, bool nullable) : Type(type_id, nullable) {}
};

class SmallIntType : public NumberBaseType<i16> {
 public:
  static const SmallIntType &InstanceNonNullable();

  static const SmallIntType &InstanceNullable();

  static const SmallIntType &Instance(bool nullable) { return (nullable ? InstanceNullable() : InstanceNonNullable()); }

  const Type &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const Type &GetNullableVersion() const override { return InstanceNullable(); }

  std::string GetName() const override;

  bool Equals(const Type &other) const override;

  static bool classof(const Type *type) { return type->type_id() == TypeId::SmallInt; }

 private:
  explicit SmallIntType(bool nullable);
};

class IntegerType : public NumberBaseType<i32> {
 public:
  static const IntegerType &InstanceNonNullable();

  static const IntegerType &InstanceNullable();

  static const IntegerType &Instance(bool nullable) { return (nullable ? InstanceNullable() : InstanceNonNullable()); }

  const Type &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const Type &GetNullableVersion() const override { return InstanceNullable(); }

  std::string GetName() const override;

  bool Equals(const Type &other) const override;

  static bool classof(const Type *type) { return type->type_id() == TypeId::Integer; }

 private:
  explicit IntegerType(bool nullable);
};

class BigIntType : public NumberBaseType<i64> {
 public:
  static const BigIntType &InstanceNonNullable();

  static const BigIntType &InstanceNullable();

  static const BigIntType &Instance(bool nullable) { return (nullable ? InstanceNullable() : InstanceNonNullable()); }

  const Type &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const Type &GetNullableVersion() const override { return InstanceNullable(); }

  std::string GetName() const override;

  bool Equals(const Type &other) const override;

  static bool classof(const Type *type) { return type->type_id() == TypeId::BigInt; }

 private:
  explicit BigIntType(bool nullable);
};

class DecimalType : public Type {
 public:
  static const DecimalType &InstanceNonNullable(u32 precision, u32 scale);

  static const DecimalType &InstanceNullable(u32 precision, u32 scale);

  static const DecimalType &Instance(bool nullable, u32 precision, u32 scale) {
    return (nullable ? InstanceNullable(precision, scale) : InstanceNonNullable(precision, scale));
  }

  const Type &GetNonNullableVersion() const override { return InstanceNonNullable(precision(), scale()); }

  const Type &GetNullableVersion() const override { return InstanceNullable(precision(), scale()); }

  std::string GetName() const override;

  bool Equals(const Type &other) const override;

  bool IsArithmetic() const override;

  u32 precision() const;

  u32 scale() const;

  static bool classof(const Type *type) { return type->type_id() == TypeId::Decimal; }

 private:
  DecimalType(bool nullable, u32 precision, u32 scale);

  template <bool nullable>
  static const DecimalType &InstanceInternal(u32 precision, u32 scale);

 private:
  u32 precision_;
  u32 scale_;
};

class DateType : public Type {
 public:
  static const DateType &InstanceNonNullable();

  static const DateType &InstanceNullable();

  static const DateType &Instance(bool nullable) { return (nullable ? InstanceNullable() : InstanceNonNullable()); }

  const Type &GetNonNullableVersion() const override { return InstanceNonNullable(); }

  const Type &GetNullableVersion() const override { return InstanceNullable(); }

  std::string GetName() const override;

  bool Equals(const Type &other) const override;

  bool IsArithmetic() const override { return false; }

  static bool classof(const Type *type) { return type->type_id() == TypeId::Date; }

 private:
  explicit DateType(bool nullable);
};

class CharType : public Type {
 public:
  static const CharType &InstanceNonNullable(u32 len);

  static const CharType &InstanceNullable(u32 len);

  static const CharType &Instance(bool nullable, u32 len) {
    return (nullable ? InstanceNullable(len) : InstanceNonNullable(len));
  }

  const Type &GetNonNullableVersion() const override { return InstanceNonNullable(length()); }

  const Type &GetNullableVersion() const override { return InstanceNullable(length()); }

  std::string GetName() const override;

  bool Equals(const Type &other) const override;

  bool IsArithmetic() const override { return false; }

  u32 length() const;

  static bool classof(const Type *type) { return type->type_id() == TypeId::Char; }

 private:
  explicit CharType(bool nullable, u32 length);

  template <bool nullable>
  static const CharType &InstanceInternal(u32 length);

 private:
  u32 length_;
};

class VarcharType : public Type {
 public:
  static const VarcharType &InstanceNonNullable(u32 max_len);

  static const VarcharType &InstanceNullable(u32 max_len);

  static const VarcharType &Instance(bool nullable, u32 max_len) {
    return (nullable ? InstanceNullable(max_len) : InstanceNonNullable(max_len));
  }

  const Type &GetNonNullableVersion() const override { return InstanceNonNullable(max_length()); }

  const Type &GetNullableVersion() const override { return InstanceNullable(max_length()); }

  std::string GetName() const override;

  bool Equals(const Type &other) const override;

  bool IsArithmetic() const override { return false; }

  u32 max_length() const;

  static bool classof(const Type *type) { return type->type_id() == TypeId::Varchar; }

 private:
  explicit VarcharType(bool nullable, u32 max_len);

  template <bool nullable>
  static const VarcharType &InstanceInternal(u32 length);

 private:
  u32 max_len_;
};

}  // namespace tpl::sql
