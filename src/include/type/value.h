#pragma once

#include <algorithm>
#include <cstring>
#include "common/hash_util.h"
#include "gtest/gtest_prod.h"
#include "type/type_id.h"
#include "type/type_util.h"

namespace terrier::parser {
class ConstantValueExpression;
}

namespace terrier::type {
class ValueFactory;
class ValuePeeker;

class TransientValue {
  friend class ValueFactory;
  friend class ValuePeeker;
  friend class terrier::parser::ConstantValueExpression;  // This is because it calls the private copy constructor for
                                                          // Value which we don't really want to expose due to its
                                                          // likelihood of being abused (it calls malloc)

 public:
  TypeId Type() const { return static_cast<TypeId>(static_cast<uint8_t>(type_) & 0x7F); }

  TransientValue() = delete;

  bool Null() const { return static_cast<bool>(static_cast<uint8_t>(type_) & 0x80); }

  void SetNull(const bool null) {
    if (null) {
      type_ = static_cast<TypeId>(static_cast<uint8_t>(type_) | 0x80);
    } else {
      type_ = static_cast<TypeId>(static_cast<uint8_t>(type_) & 0x7F);
    }
  }

  ~TransientValue() {
    if (Type() == TypeId::VARCHAR) {
      delete[] reinterpret_cast<char *const>(data_);
    }
  }

  bool operator==(const TransientValue &rhs) const {
    if (type_ != rhs.type_) return false;
    if (type_ != TypeId::VARCHAR) return data_ == rhs.data_;

    const char *const varchar = reinterpret_cast<const char *const>(data_);
    const uint32_t length = *reinterpret_cast<const uint32_t *const>(varchar);

    const char *const rhs_varchar = reinterpret_cast<const char *const>(rhs.data_);
    const uint32_t rhs_length = *reinterpret_cast<const uint32_t *const>(rhs_varchar);

    if (length != rhs_length) return false;

    const char *const varchar_contents = varchar + sizeof(uint32_t);
    const char *const rhs_varchar_contents = rhs_varchar + sizeof(uint32_t);

    return std::memcmp(varchar_contents, rhs_varchar_contents, length) == 0;
  }

  bool operator!=(const TransientValue &rhs) const { return !(operator==(rhs)); }

  common::hash_t Hash() const {
    if (type_ != TypeId::VARCHAR)
      return common::HashUtil::HashBytes(reinterpret_cast<const byte *const>(&data_), sizeof(uintptr_t));

    const uint32_t length = *reinterpret_cast<const uint32_t *const>(data_);
    return common::HashUtil::HashBytes(reinterpret_cast<const byte *const>(data_), length + sizeof(uint32_t));
  }

 private:
  FRIEND_TEST(ValueTests, BooleanTest);
  FRIEND_TEST(ValueTests, TinyIntTest);
  FRIEND_TEST(ValueTests, SmallIntTest);
  FRIEND_TEST(ValueTests, IntegerTest);
  FRIEND_TEST(ValueTests, BigIntTest);
  FRIEND_TEST(ValueTests, DecimalTest);
  FRIEND_TEST(ValueTests, TimestampTest);
  FRIEND_TEST(ValueTests, DateTest);
  FRIEND_TEST(ValueTests, VarCharTest);

  template <typename T>
  TransientValue(const TypeId type, T data) {
    // clear internal buffer
    data_ = 0;
    type_ = type;
    const auto num_bytes = std::min(static_cast<uint8_t>(static_cast<uint8_t>(TypeUtil::GetTypeSize(type)) & 0x7F),
                                    static_cast<uint8_t>(sizeof(uintptr_t)));
    std::memcpy(&data_, &data, num_bytes);
  }

  TransientValue(const TransientValue &other) {
    // clear internal buffer
    data_ = 0;
    type_ = other.type_;
    if (Type() != TypeId::VARCHAR) {
      data_ = other.data_;
    } else {
      CopyVarChar(reinterpret_cast<const char *const>(other.data_));
    }
  }

  TransientValue &operator=(const TransientValue &other) {
    if (this != &other) {  // self-assignment check expected
      if (Type() == TypeId::VARCHAR) {
        // free VARCHAR buffer
        delete[] reinterpret_cast<char *const>(data_);
      }
      // clear internal buffer
      data_ = 0;
      type_ = other.type_;
      if (Type() != TypeId::VARCHAR) {
        data_ = other.data_;
      } else {
        CopyVarChar(reinterpret_cast<const char *const>(other.data_));
      }
    }
    return *this;
  }

  template <typename T>
  T GetAs() const {
    return *reinterpret_cast<const T *const>(&data_);
  }

  void CopyVarChar(const char *const other) {
    // allocate a VARCHAR buffer
    const char *const other_varchar = reinterpret_cast<const char *const>(other);
    const uint32_t length = *reinterpret_cast<const uint32_t *const>(other_varchar);
    char *const varchar = new char[length + sizeof(uint32_t)];

    // copy the length field into the VARCHAR buffer
    *(reinterpret_cast<uint32_t *const>(varchar)) = length;

    // copy the VARCHAR contents into the VARCHAR buffer
    char *const varchar_contents = varchar + sizeof(uint32_t);
    const char *const other_varchar_contents = other_varchar + sizeof(uint32_t);
    std::memcpy(varchar_contents, other_varchar_contents, length);
    // copy the pointer to the VARCHAR buffer into the internal buffer
    data_ = reinterpret_cast<uintptr_t>(varchar);
  }

  TypeId type_ = TypeId::INVALID;
  uintptr_t data_;
};

}  // namespace terrier::type
