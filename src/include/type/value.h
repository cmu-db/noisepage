#pragma once

#include <algorithm>
#include <cstring>
#include "common/hash_util.h"
#include "type/type_id.h"
#include "type/type_util.h"

namespace terrier::type {
class ValueFactory;
class ValuePeeker;

constexpr uint8_t VALUE_SIZE = 8;

class TransientValue {
  friend class ValueFactory;
  friend class ValuePeeker;

 public:
  TypeId Type() const { return static_cast<TypeId>(static_cast<uint8_t>(type_) & 0x7F); }

  TransientValue() = delete;

  TransientValue(const TransientValue &other) {
    // clear internal buffer
    std::memset(data_, 0, VALUE_SIZE);
    type_ = other.type_;
    if (Type() != TypeId::VARCHAR) {
      // copy buffer
      std::memcpy(data_, other.data_, VALUE_SIZE);
    } else {
      // allocate a VARCHAR buffer
      const char *const other_varchar = *reinterpret_cast<const char *const *const>(other.data_);
      const uint32_t length = *reinterpret_cast<const uint32_t *const>(other_varchar);
      char *const varchar = new char[length + sizeof(uint32_t)];

      // copy the length field into the VARCHAR buffer
      *(reinterpret_cast<uint32_t *const>(varchar)) = length;

      // copy the VARCHAR contents into the VARCHAR buffer
      char *const varchar_contents = varchar + sizeof(uint32_t);
      const char *const other_varchar_contents = other_varchar + sizeof(uint32_t);
      std::memcpy(varchar_contents, other_varchar_contents, length);
      // copy the pointer to the VARCHAR buffer into the internal buffer
      std::memcpy(data_, &varchar, VALUE_SIZE);
    }
  }

  TransientValue &operator=(const TransientValue &other) {
    if (this != &other) {  // self-assignment check expected
      if (Type() == TypeId::VARCHAR) {
        // free VARCHAR buffer
        delete[] * reinterpret_cast<char *const *const>(data_);
      }
      // clear internal buffer
      std::memset(data_, 0, VALUE_SIZE);
      type_ = other.type_;
      if (Type() != TypeId::VARCHAR) {
        // copy buffer
        std::memcpy(data_, other.data_, VALUE_SIZE);
      } else {
        // allocate a VARCHAR buffer
        const char *const other_varchar = *reinterpret_cast<const char *const *const>(other.data_);
        const uint32_t length = *reinterpret_cast<const uint32_t *const>(other_varchar);
        char *const varchar = new char[length + sizeof(uint32_t)];

        // copy the length field into the VARCHAR buffer
        *(reinterpret_cast<uint32_t *const>(varchar)) = length;

        // copy the VARCHAR contents into the VARCHAR buffer
        char *const varchar_contents = varchar + sizeof(uint32_t);
        const char *const other_varchar_contents = other_varchar + sizeof(uint32_t);
        std::memcpy(varchar_contents, other_varchar_contents, length);
        // copy the pointer to the VARCHAR buffer into the internal buffer
        std::memcpy(data_, &varchar, VALUE_SIZE);
      }
    }
    return *this;
  }

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
      delete[] * reinterpret_cast<char *const *const>(data_);
    }
  }

  bool operator==(const TransientValue &rhs) const {
    if (type_ != rhs.type_) return false;
    if (type_ != TypeId::VARCHAR) return std::memcmp(data_, rhs.data_, VALUE_SIZE) == 0;

    const char *const varchar = *reinterpret_cast<const char *const *const>(data_);
    const uint32_t length = *reinterpret_cast<const uint32_t *const>(varchar);

    const char *const rhs_varchar = *reinterpret_cast<const char *const *const>(rhs.data_);
    const uint32_t rhs_length = *reinterpret_cast<const uint32_t *const>(rhs_varchar);

    if (length != rhs_length) return false;

    const char *const varchar_contents = varchar + sizeof(uint32_t);
    const char *const rhs_varchar_contents = rhs_varchar + sizeof(uint32_t);

    return std::memcmp(varchar_contents, rhs_varchar_contents, length) == 0;
  }

  bool operator!=(const TransientValue &rhs) const { return !(operator==(rhs)); }

  common::hash_t Hash() const {
    if (type_ != TypeId::VARCHAR) return common::HashUtil::HashBytes(data_, VALUE_SIZE);

    const uint32_t length = *reinterpret_cast<const uint32_t *const>(data_);
    return common::HashUtil::HashBytes(data_, length + sizeof(uint32_t));
  }

 private:
  template <typename T>
  TransientValue(const TypeId type, T data) {
    // clear internal buffer
    std::memset(data_, 0, VALUE_SIZE);
    type_ = type;
    const auto num_bytes =
        std::min(static_cast<uint8_t>(static_cast<uint8_t>(TypeUtil::GetTypeSize(type)) & 0x7F), VALUE_SIZE);
    std::memcpy(data_, &data, num_bytes);
  }

  template <typename T>
  T GetAs() const {
    return *reinterpret_cast<const T *const>(data_);
  }

  TypeId type_ = TypeId::INVALID;
  byte data_[VALUE_SIZE];
};

}  // namespace terrier::type
