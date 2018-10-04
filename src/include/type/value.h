#pragma once

#include <cstdint>
#include <string>
#include "common/hash_util.h"
#include "common/typedefs.h"
#include "type/type_id.h"

namespace terrier::type {

/**
 * Container for a variable type value.
 */
class Value {
 public:
  ~Value();
  /**
   * Create a copy of value other
   * @param other
   */
  Value(const Value &other);

  /**
   * Create a boolean value
   * @param value
   */
  explicit Value(boolean_t value);
  /**
   * create a tinyint value
   * @param value
   */
  explicit Value(int8_t value);
  /**
   * create a smallint value
   * @param value
   */
  explicit Value(int16_t value);
  /**
   * Create an integer value
   * @param value
   */
  explicit Value(int32_t value);
  /**
   * Create a bigint value
   * @param value
   */
  explicit Value(int64_t value);
  /**
   * Create a double value
   * @param value
   */
  explicit Value(double value);
  /**
   * Create a timestamp value
   * @param value
   */
  explicit Value(timestamp_t value);
  /**
   * Create a date value
   * @param value
   */
  explicit Value(date_t value);

  /**
   * Create a varchar value. Will be stored without a null terminator, with adjusted length.
   * @param value - string.
   */
  explicit Value(const std::string &value);

  /**
   * Create a varbinary value
   * @param data of the value
   * @param len in bytes of the data
   */
  Value(const char *data, uint32_t len);

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
  const boolean_t *GetBooleanValue() const {
    TERRIER_ASSERT(type_id_ == TypeId::BOOLEAN, "The type must be a boolean");
    return &value_.boolean;
  }

  /**
   * Get contents of a tinyint value
   * @return int8_t
   */
  const int8_t *GetTinyIntValue() const {
    TERRIER_ASSERT(type_id_ == TypeId::TINYINT, "The type must be a tinyint");
    return &value_.tinyint;
  }

  /**
   * Get contents of a smallint value
   * @return int16_t
   */
  const int16_t *GetSmallIntValue() const {
    TERRIER_ASSERT(type_id_ == TypeId::SMALLINT, "The type must be a smallint");
    return &value_.smallint;
  }

  /**
   * Get contents of an integer value
   * @return int32_t
   */
  const int32_t *GetIntValue() const {
    TERRIER_ASSERT(type_id_ == TypeId::INTEGER, "The type must be a integer");
    return &value_.integer;
  }

  /**
   * Get contents of a bigint value
   * @return int64_t
   */
  const int64_t *GetBigIntValue() const {
    TERRIER_ASSERT(type_id_ == TypeId::BIGINT, "The type must be a bigint");
    return &value_.bigint;
  }

  /**
   * Get contents of a decimal value
   * @return double
   */
  const double *GetDecimalValue() const {
    TERRIER_ASSERT(type_id_ == TypeId::DECIMAL, "The type must be a decimal");
    return &value_.decimal;
  }

  /**
   * Get contents of a timestamp value
   * @return timestamp_t
   */
  const timestamp_t *GetTimestampValue() const {
    TERRIER_ASSERT(type_id_ == TypeId::TIMESTAMP, "The type must be a timestamp");
    return &value_.timestamp;
  }

  /**
   * Get contents of a date value
   * @return date_t
   */
  const date_t *GetDateValue() const {
    TERRIER_ASSERT(type_id_ == TypeId::DATE, "The type must be a date");
    return &value_.date;
  }

  /**
   * Compare values for equality
   * @param rhs
   * @return true if equal, false if not.
   */
  bool operator==(const Value &rhs) const;

  /**
   * Hash the value
   * @return hashed value
   */
  hash_t Hash() const {
    hash_t ret_hash = 0;

    switch (type_id_) {
      case TypeId::BOOLEAN:
        ret_hash = HashUtil::Hash(GetBooleanValue());
        break;

      case TypeId::TINYINT:
        ret_hash = HashUtil::Hash(GetTinyIntValue());
        break;

      case TypeId::SMALLINT:
        ret_hash = HashUtil::Hash(GetSmallIntValue());
        break;

      case TypeId::INTEGER:
        ret_hash = HashUtil::Hash(GetIntValue());
        break;

      case TypeId::BIGINT:
        ret_hash = HashUtil::Hash(GetBigIntValue());
        break;

      case TypeId::DATE:
        ret_hash = HashUtil::Hash(GetDateValue());
        break;

      case TypeId::DECIMAL:
        ret_hash = HashUtil::Hash(GetDecimalValue());
        break;

      case TypeId::TIMESTAMP:
        ret_hash = HashUtil::Hash(GetTimestampValue());
        break;

      default:
        TERRIER_ASSERT(false, "unsupported type");
        break;
    }
    return ret_hash;
  }

 protected:
  union Val {
    /**
     * booleans
     */
    boolean_t boolean;
    /**
     * tiny integers
     */
    int8_t tinyint;
    /**
     * small integers
     */
    int16_t smallint;
    /**
     * Integers
     */
    int32_t integer;
    /**
     * Big integers
     */
    int64_t bigint;
    /**
     * Decimals, i.e. floating point values
     */
    double decimal;
    /**
     * Timestamps
     */
    timestamp_t timestamp;
    /**
     * Date values
     */
    date_t date;
    /**
     * variable length strings, without a null terminator. Length specified by var_len
     */
    char *varchar;
    // const char *const_varlen;
    // char *array;
  } value_;

  // For variable length types
  uint32_t var_len;

  // type of this Value
  TypeId type_id_;
};

}  // namespace terrier::type
