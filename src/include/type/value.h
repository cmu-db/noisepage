#pragma once

#include <algorithm>
#include <cstring>
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

 private:
  template <typename T>
  TransientValue(const TypeId type, T data) {
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
