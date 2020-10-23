#include "execution/sql/data_types.h"

#include <llvm/ADT/DenseMap.h>

#include <memory>
#include <string>
#include <utility>

namespace noisepage::execution::sql {

// ---------------------------------------------------------
// Boolean
// ---------------------------------------------------------

BooleanType::BooleanType(bool nullable) : SqlType(SqlTypeId::Boolean, nullable) {}

TypeId BooleanType::GetPrimitiveTypeId() const { return TypeId::Boolean; }

std::string BooleanType::GetName() const {
  std::string str = "Boolean";
  if (IsNullable()) {
    str.append("[NULLABLE]");
  }
  return str;
}

bool BooleanType::IsIntegral() const { return false; }

bool BooleanType::IsFloatingPoint() const { return false; }

bool BooleanType::IsArithmetic() const { return false; }

bool BooleanType::Equals(const SqlType &that) const {
  return that.Is<BooleanType>() && IsNullable() == that.IsNullable();
}

const BooleanType &BooleanType::InstanceNonNullable() {
  static BooleanType k_non_nullable_boolean(false);
  return k_non_nullable_boolean;
}

const BooleanType &BooleanType::InstanceNullable() {
  static BooleanType k_nullable_boolean(true);
  return k_nullable_boolean;
}

// ---------------------------------------------------------
// Tiny Integer
// ---------------------------------------------------------

TinyIntType::TinyIntType(bool nullable) : NumberBaseType(SqlTypeId::TinyInt, nullable) {}

std::string TinyIntType::GetName() const {
  std::string str = "TinyInt";
  if (IsNullable()) {
    str.append("[NULLABLE]");
  }
  return str;
}

bool TinyIntType::Equals(const SqlType &that) const {
  return that.Is<TinyIntType>() && IsNullable() == that.IsNullable();
}

const TinyIntType &TinyIntType::InstanceNonNullable() {
  static TinyIntType k_non_nullable_tiny_int(false);
  return k_non_nullable_tiny_int;
}

const TinyIntType &TinyIntType::InstanceNullable() {
  static TinyIntType k_nullable_tiny_int(true);
  return k_nullable_tiny_int;
}

// ---------------------------------------------------------
// Small Integer
// ---------------------------------------------------------

SmallIntType::SmallIntType(bool nullable) : NumberBaseType(SqlTypeId::SmallInt, nullable) {}

std::string SmallIntType::GetName() const {
  std::string str = "SmallInt";
  if (IsNullable()) {
    str.append("[NULLABLE]");
  }
  return str;
}

bool SmallIntType::Equals(const SqlType &that) const {
  return that.Is<SmallIntType>() && IsNullable() == that.IsNullable();
}

const SmallIntType &SmallIntType::InstanceNonNullable() {
  static SmallIntType k_non_nullable_small_int(false);
  return k_non_nullable_small_int;
}

const SmallIntType &SmallIntType::InstanceNullable() {
  static SmallIntType k_nullable_small_int(true);
  return k_nullable_small_int;
}

// ---------------------------------------------------------
// Integer
// ---------------------------------------------------------

IntegerType::IntegerType(bool nullable) : NumberBaseType(SqlTypeId::Integer, nullable) {}

std::string IntegerType::GetName() const {
  std::string str = "Integer";
  if (IsNullable()) {
    str.append("[NULLABLE]");
  }
  return str;
}

bool IntegerType::Equals(const SqlType &that) const {
  return that.Is<IntegerType>() && IsNullable() == that.IsNullable();
}

const IntegerType &IntegerType::InstanceNonNullable() {
  static IntegerType k_non_nullable_int(false);
  return k_non_nullable_int;
}

const IntegerType &IntegerType::InstanceNullable() {
  static IntegerType k_nullable_int(true);
  return k_nullable_int;
}

// ---------------------------------------------------------
// Big Integer
// ---------------------------------------------------------

BigIntType::BigIntType(bool nullable) : NumberBaseType(SqlTypeId::BigInt, nullable) {}

std::string BigIntType::GetName() const {
  std::string str = "BigInt";
  if (IsNullable()) {
    str.append("[NULLABLE]");
  }
  return str;
}

bool BigIntType::Equals(const SqlType &that) const {
  return that.Is<BigIntType>() && IsNullable() == that.IsNullable();
}

const BigIntType &BigIntType::InstanceNonNullable() {
  static BigIntType k_non_nullable_big_int(false);
  return k_non_nullable_big_int;
}

const BigIntType &BigIntType::InstanceNullable() {
  static BigIntType k_nullable_big_int(true);
  return k_nullable_big_int;
}

// ---------------------------------------------------------
// Real
// ---------------------------------------------------------

RealType::RealType(bool nullable) : NumberBaseType(SqlTypeId::Real, nullable) {}

std::string RealType::GetName() const {
  std::string str = "Real";
  if (IsNullable()) {
    str.append("[NULLABLE]");
  }
  return str;
}

bool RealType::Equals(const SqlType &that) const { return that.Is<RealType>() && IsNullable() == that.IsNullable(); }

const RealType &RealType::InstanceNonNullable() {
  static RealType k_non_nullable_real(false);
  return k_non_nullable_real;
}

const RealType &RealType::InstanceNullable() {
  static RealType k_nullable_real(true);
  return k_nullable_real;
}

// ---------------------------------------------------------
// Double
// ---------------------------------------------------------

DoubleType::DoubleType(bool nullable) : NumberBaseType(SqlTypeId::Double, nullable) {}

std::string DoubleType::GetName() const {
  std::string str = "Double";
  if (IsNullable()) {
    str.append("[NULLABLE]");
  }
  return str;
}

bool DoubleType::Equals(const SqlType &that) const {
  return that.Is<DoubleType>() && IsNullable() == that.IsNullable();
}

const DoubleType &DoubleType::InstanceNonNullable() {
  static DoubleType k_non_nullable_double(false);
  return k_non_nullable_double;
}

const DoubleType &DoubleType::InstanceNullable() {
  static DoubleType k_nullable_double(true);
  return k_nullable_double;
}

// ---------------------------------------------------------
// Decimal
// ---------------------------------------------------------

DecimalType::DecimalType(bool nullable, uint32_t precision, uint32_t scale)
    : SqlType(SqlTypeId::Decimal, nullable), precision_(precision), scale_(scale) {}

TypeId DecimalType::GetPrimitiveTypeId() const { return TypeId::BigInt; }

std::string DecimalType::GetName() const {
  std::string str = "Decimal[" + std::to_string(Precision()) + "," + std::to_string(Scale());
  if (IsNullable()) {
    str.append(",NULLABLE");
  }
  str.append("]");
  return str;
}

bool DecimalType::Equals(const SqlType &that) const {
  if (auto *other_decimal = that.SafeAs<DecimalType>()) {
    return Precision() == other_decimal->Precision() && Scale() == other_decimal->Scale() &&
           IsNullable() == that.IsNullable();
  }
  return false;
}

bool DecimalType::IsIntegral() const { return false; }

bool DecimalType::IsFloatingPoint() const { return true; }

bool DecimalType::IsArithmetic() const { return true; }

uint32_t DecimalType::Precision() const { return precision_; }

uint32_t DecimalType::Scale() const { return scale_; }

template <bool Nullable>
const DecimalType &DecimalType::InstanceInternal(uint32_t precision, uint32_t scale) {
  static llvm::DenseMap<std::pair<uint32_t, uint32_t>, std::unique_ptr<DecimalType>> k_decimal_type_map;

  auto key = std::make_pair(precision, scale);
  if (auto iter = k_decimal_type_map.find(key); iter != k_decimal_type_map.end()) {
    return *iter->second;
  }

  auto iter = k_decimal_type_map.try_emplace(key, new DecimalType(Nullable, precision, scale));
  return *iter.first->second;
}

const DecimalType &DecimalType::InstanceNonNullable(uint32_t precision, uint32_t scale) {
  return InstanceInternal<false>(precision, scale);
}

const DecimalType &DecimalType::InstanceNullable(uint32_t precision, uint32_t scale) {
  return InstanceInternal<true>(precision, scale);
}

// ---------------------------------------------------------
// Date
// ---------------------------------------------------------

const DateType &DateType::InstanceNonNullable() {
  static DateType k_non_nullable_date(false);
  return k_non_nullable_date;
}

const DateType &DateType::InstanceNullable() {
  static DateType k_nullable_date(true);
  return k_nullable_date;
}

TypeId DateType::GetPrimitiveTypeId() const { return TypeId::Date; }

std::string DateType::GetName() const {
  std::string str = "Date";
  if (IsNullable()) {
    str.append("[NULLABLE]");
  }
  return str;
}

bool DateType::Equals(const SqlType &that) const { return that.Is<DateType>() && IsNullable() == that.IsNullable(); }

DateType::DateType(bool nullable) : SqlType(SqlTypeId::Date, nullable) {}

// ---------------------------------------------------------
// Timestamp
// ---------------------------------------------------------

const TimestampType &TimestampType::InstanceNonNullable() {
  static TimestampType k_non_nullable_timestamp(false);
  return k_non_nullable_timestamp;
}

const TimestampType &TimestampType::InstanceNullable() {
  static TimestampType k_nullable_timestamp(true);
  return k_nullable_timestamp;
}

TypeId TimestampType::GetPrimitiveTypeId() const { return TypeId::Timestamp; }

std::string TimestampType::GetName() const {
  std::string str = "Timestamp";
  if (IsNullable()) {
    str.append("[NULLABLE]");
  }
  return str;
}

bool TimestampType::Equals(const SqlType &that) const {
  return that.Is<TimestampType>() && IsNullable() && that.IsNullable();
}

TimestampType::TimestampType(bool nullable) : SqlType(SqlTypeId::Timestamp, nullable) {}

// ---------------------------------------------------------
// Fixed-length strings
// ---------------------------------------------------------

template <bool Nullable>
const CharType &CharType::InstanceInternal(uint32_t length) {
  static llvm::DenseMap<uint32_t, std::unique_ptr<CharType>> k_char_type_map;

  if (auto iter = k_char_type_map.find(length); iter != k_char_type_map.end()) {
    return *iter->second;
  }

  auto iter = k_char_type_map.try_emplace(length, new CharType(Nullable, length));
  return *iter.first->second;
}

const CharType &CharType::InstanceNonNullable(uint32_t len) { return InstanceInternal<false>(len); }
const CharType &CharType::InstanceNullable(uint32_t len) { return InstanceInternal<true>(len); }

CharType::CharType(bool nullable, uint32_t length) : SqlType(SqlTypeId::Char, nullable), length_(length) {}

TypeId CharType::GetPrimitiveTypeId() const { return TypeId::Varchar; }

std::string CharType::GetName() const {
  std::string str = "Char[" + std::to_string(Length());
  if (IsNullable()) {
    str.append(",NULLABLE");
  }
  str.append("]");
  return str;
}

bool CharType::Equals(const SqlType &that) const {
  if (auto *other_char = that.SafeAs<CharType>()) {
    return Length() == other_char->Length() && IsNullable() == other_char->IsNullable();
  }
  return false;
}

uint32_t CharType::Length() const { return length_; }

// ---------------------------------------------------------
// Variable-length strings
// ---------------------------------------------------------

template <bool Nullable>
const VarcharType &VarcharType::InstanceInternal(uint32_t length) {
  static llvm::DenseMap<uint32_t, std::unique_ptr<VarcharType>> k_varchar_type_map;

  if (auto iter = k_varchar_type_map.find(length); iter != k_varchar_type_map.end()) {
    return *iter->second;
  }

  auto iter = k_varchar_type_map.try_emplace(length, new VarcharType(Nullable, length));
  return *iter.first->second;
}

const VarcharType &VarcharType::InstanceNonNullable(uint32_t max_len) { return InstanceInternal<false>(max_len); }

const VarcharType &VarcharType::InstanceNullable(uint32_t max_len) { return InstanceInternal<true>(max_len); }

VarcharType::VarcharType(bool nullable, uint32_t max_len) : SqlType(SqlTypeId::Varchar, nullable), max_len_(max_len) {}

TypeId VarcharType::GetPrimitiveTypeId() const { return TypeId::Varchar; }

std::string VarcharType::GetName() const {
  std::string str = "Varchar[" + std::to_string(MaxLength());
  if (IsNullable()) {
    str.append(",NULLABLE");
  }
  str.append("]");
  return str;
}

bool VarcharType::Equals(const SqlType &that) const {
  if (auto *other_varchar = that.SafeAs<VarcharType>()) {
    return MaxLength() == other_varchar->MaxLength() && IsNullable() == other_varchar->IsNullable();
  }
  return false;
}

uint32_t VarcharType::MaxLength() const { return max_len_; }

}  // namespace noisepage::execution::sql
