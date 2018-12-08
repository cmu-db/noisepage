#pragma once

#include <cstdint>
#include <string>
#include "common/hash_util.h"
#include "common/strong_typedef.h"
#include "type/type_id.h"

namespace terrier::type {
class ValueFactory;
/**
 * Container for a variable type value.
 */
class Value {
  friend class ValueFactory;

 public:
  /**
   * Destructs a value
   */
  // TODO(Tianyu): Should the value object be responsible for deallocation of varlen elements it hold?
  ~Value() = default;
  /**
   * Encapsulates length and data of a varlen entry
   */
  struct VarlenValue {
    /**
     * Length of the varlen value
     */
    uint64_t size_;
    /**
     * Pointer to the content of the varlen entry, NOT nul-terminated. Its length is as specified in size_.
     */
    byte *data_;
  };

  /**
   * Get the type of this value
   * @return TypeId
   */
  TypeId GetType() const { return type_id_; }

  // value retrieval methods
  /**
   * Get contents of a boolean value
   * @return boolean_t
   */
  const bool &GetBooleanValue() const {
    TERRIER_ASSERT(type_id_ == TypeId::BOOLEAN, "The type must be a boolean");
    return value_.boolean_;
  }

  /**
   * Get contents of a tinyint value
   * @return int8_t
   */
  const int8_t &GetTinyIntValue() const {
    TERRIER_ASSERT(type_id_ == TypeId::TINYINT, "The type must be a tinyint");
    return value_.tinyint_;
  }

  /**
   * Get contents of a smallint value
   * @return int16_t
   */
  const int16_t &GetSmallIntValue() const {
    TERRIER_ASSERT(type_id_ == TypeId::SMALLINT, "The type must be a smallint");
    return value_.smallint_;
  }

  /**
   * Get contents of an integer value
   * @return int32_t
   */
  const int32_t &GetIntValue() const {
    TERRIER_ASSERT(type_id_ == TypeId::INTEGER, "The type must be a integer");
    return value_.integer_;
  }

  /**
   * Get contents of a bigint value
   * @return int64_t
   */
  const int64_t &GetBigIntValue() const {
    TERRIER_ASSERT(type_id_ == TypeId::BIGINT, "The type must be a bigint");
    return value_.bigint_;
  }

  /**
   * Get contents of a decimal value
   * @return double
   */
  const double &GetDecimalValue() const {
    TERRIER_ASSERT(type_id_ == TypeId::DECIMAL, "The type must be a decimal");
    return value_.decimal_;
  }

  /**
   * Get contents of a timestamp value
   * @return timestamp_t
   */
  const timestamp_t &GetTimestampValue() const {
    TERRIER_ASSERT(type_id_ == TypeId::TIMESTAMP, "The type must be a timestamp");
    return value_.timestamp_;
  }

  /**
   * Get contents of a date value
   * @return date_t
   */
  const date_t &GetDateValue() const {
    TERRIER_ASSERT(type_id_ == TypeId::DATE, "The type must be a date");
    return value_.date_;
  }

  /**
   * Compare values for equality
   * @param rhs the value to compare to
   * @return true if equal, false if not.
   */
  bool operator==(const Value &rhs) const {
    if (type_id_ != rhs.type_id_) return false;
    switch (type_id_) {
      case TypeId::BOOLEAN:
        return value_.boolean_ == rhs.value_.boolean_;
      case TypeId::TINYINT:
        return value_.tinyint_ == rhs.value_.tinyint_;
      case TypeId::SMALLINT:
        return value_.smallint_ == rhs.value_.smallint_;
      case TypeId::INTEGER:
        return value_.integer_ == rhs.value_.integer_;
      case TypeId::BIGINT:
        return value_.bigint_ == rhs.value_.bigint_;
      case TypeId::DATE:
        return value_.date_ == rhs.value_.date_;
      case TypeId::DECIMAL:
        return value_.decimal_ == rhs.value_.decimal_;
      case TypeId::TIMESTAMP:
        return value_.timestamp_ == rhs.value_.timestamp_;
      default:
        TERRIER_ASSERT(false, "unsupported type");
        throw std::runtime_error("unreachable control flow");
    }
  }

  /**
   * Inequality check
   * @param rhs the value to compare to
   * @return true if not not equal, false otherwise
   */
  bool operator!=(const Value &rhs) const { return operator==(rhs); }

  /**
   * Hash the value
   * @return hashed value
   */
  common::hash_t Hash() const {
    switch (type_id_) {
      case TypeId::BOOLEAN:
        return common::HashUtil::Hash(GetBooleanValue());
      case TypeId::TINYINT:
        return common::HashUtil::Hash(GetTinyIntValue());
      case TypeId::SMALLINT:
        return common::HashUtil::Hash(GetSmallIntValue());
      case TypeId::INTEGER:
        return common::HashUtil::Hash(GetIntValue());
      case TypeId::BIGINT:
        return common::HashUtil::Hash(GetBigIntValue());
      case TypeId::DATE:
        return common::HashUtil::Hash(GetDateValue());
      case TypeId::DECIMAL:
        return common::HashUtil::Hash(GetDecimalValue());
      case TypeId::TIMESTAMP:
        return common::HashUtil::Hash(GetTimestampValue());
      case TypeId::VARBINARY:
      case TypeId::VARCHAR:
        return common::HashUtil::HashBytes(value_.varlen_.data_, value_.varlen_.size_);
      default:
        TERRIER_ASSERT(false, "unsupported type");
        throw std::runtime_error("unreachable control flow");
    }
  }

 private:
  union Val {
    bool boolean_;
    int8_t tinyint_;
    int16_t smallint_;
    int32_t integer_;
    int64_t bigint_;
    double decimal_;
    timestamp_t timestamp_;
    date_t date_;
    VarlenValue varlen_;
  };

  Value(TypeId type_id, Val val) : type_id_(type_id), value_(val) {}

  TypeId type_id_;
  Val value_;
};

}  // namespace terrier::type
