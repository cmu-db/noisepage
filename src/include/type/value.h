#pragma once

#include <cstdint>
#include <cstring>
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
   * Create a copy of a value
   * @param v the copy
   */
  Value(const Value &v) {
    type_id_ = v.type_id_;
    value_ = v.value_;
    switch (Type()) {
      case TypeId::VARCHAR: {
        if (!Null()) {
          size_t size = strlen(v.value_.string_);
          value_.string_ = static_cast<char *>(malloc(size + 1));
          memcpy(const_cast<char *>(value_.string_), v.value_.string_, size + 1);
        }
      }
        return;

      default:
        return;
    }
  }

  /**
   * Destructs a value
   */
  ~Value() {
    switch (Type()) {
      case TypeId::VARCHAR:
        if (!Null()) {
          free(const_cast<char *>(value_.string_));
        }
        value_.string_ = nullptr;
        return;

      default:
        return;
    }
  }

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
   * @return TypeId of thisValue object.
   */
  TypeId Type() const {
    // bitwise AND the TypeId with 01111111 to return TypeId value without the embedded NULL bit
    return static_cast<TypeId>(static_cast<uint8_t>(type_id_) & 0x7F);
  }
  /**
   * @return true if TransientValue is a SQL NULL, otherwise false
   */
  bool Null() const {
    // bitwise AND the TypeId with 1000000 to extract NULL bit
    return static_cast<bool>(static_cast<uint8_t>(type_id_) & 0x80);
  }

  /**
   * Change the SQL NULL value of this TransientValue. We use the MSB to reflect this since we don't need all 8 bits for
   * TypeId
   * @param set_null true if TransientValue should be set to NULL, false otherwise
   */
  void SetNull(const bool set_null) {
    if (set_null) {
      // bitwise OR the TypeId with 1000000 to set NULL bit
      type_id_ = static_cast<TypeId>(static_cast<uint8_t>(type_id_) | 0x80);
    } else {
      // bitwise AND the TypeId with 01111111 to clear NULL bit
      type_id_ = static_cast<TypeId>(static_cast<uint8_t>(type_id_) & 0x7F);
    }
  }

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
   * Get the string value
   * @return ptr to string
   */
  const char *GetVarcharValue() const {
    TERRIER_ASSERT(type_id_ == TypeId::VARCHAR, "The type must be a varchar");
    return value_.string_;
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
    switch (Type()) {
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
      case TypeId::VARCHAR: {
        return strcmp(value_.string_, rhs.value_.string_) == 0;
      }
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
  bool operator!=(const Value &rhs) const { return !operator==(rhs); }

  /**
   * Deep copy assignment operator
   * @param v the value to be assigned
   * @return the copied valueref_row[i]==row[i]);
   */
  Value &operator=(const Value &v) {
    if (this != &v) {
      if (type_id_ == type::TypeId::VARCHAR) {
        if (!Null()) free(const_cast<char *>(value_.string_));
        value_.string_ = nullptr;
      }
      type_id_ = v.type_id_;
      value_ = v.value_;
      if (v.type_id_ == type::TypeId::VARCHAR) {
        size_t size = strlen(v.value_.string_);
        if (!Null()) {
          value_.string_ = static_cast<char *>(malloc(size + 1));
          memcpy(const_cast<char *>(value_.string_), v.value_.string_, size + 1);
        }
      }
    }
    return *this;  // return the object itself (by reference)
  }

  // TODO(Yuze): Migrate to TransientValue
  bool CompareBetweenInclusive(const Value &a, const Value &b) const {
    switch (Type()) {
      case TypeId::TINYINT:
        return value_.tinyint_ >= a.GetTinyIntValue() && value_.tinyint_ <= b.GetTinyIntValue();
      case TypeId::SMALLINT:
        return value_.smallint_ >= a.GetSmallIntValue() && value_.smallint_ <= b.GetSmallIntValue();
      case TypeId::INTEGER:
        return value_.integer_ >= a.GetIntValue() && value_.integer_ <= b.GetIntValue();
      case TypeId::BIGINT:
        return value_.bigint_ >= a.GetBigIntValue() && value_.bigint_ <= b.GetBigIntValue();
      case TypeId::DECIMAL:
        return value_.decimal_ >= a.GetDecimalValue() && value_.decimal_ <= b.GetDecimalValue();
      default:
        throw std::runtime_error("unsupported comparison type");
    }
  }

  // TODO(Yuze): Migrate to TransientValue
  std::string PeekAsString() const {
    switch (Type()) {
      case TypeId::TINYINT:
        return std::to_string(value_.tinyint_);
      case TypeId::SMALLINT:
        return std::to_string(value_.smallint_);
      case TypeId::INTEGER:
        return std::to_string(value_.integer_);
      case TypeId::BIGINT:
        return std::to_string(value_.bigint_);
      case TypeId::DECIMAL:
        return std::to_string(value_.decimal_);
      default:
        throw std::runtime_error("unsupported peek type");
    }
  }

  /**
   * Hash the value
   * @return hashed value
   */
  common::hash_t Hash() const {
    switch (Type()) {
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
      case TypeId::VARCHAR:
        return common::HashUtil::Hash(GetVarcharValue());
      case TypeId::VARBINARY:
        //      case TypeId::VARCHAR:
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
    const char *string_;
    VarlenValue varlen_;
  };

  Value(TypeId type_id, Val val) : type_id_(type_id), value_(val) {}

  // NULL Value constructor
  explicit Value(TypeId type_id) : type_id_(type_id) { SetNull(true); }

  TypeId type_id_;
  Val value_;
};

}  // namespace terrier::type
