#pragma once

#include <cstring>
#include "type/type_id.h"
#include "type/type_util.h"

namespace terrier::type {
class ValueFactory;
class ValuePeeker;
class ValueWrapper;

class Value {
  friend class ValueFactory;
  friend class ValuePeeker;
  friend class ValueWrapper;

 public:
  TypeId Type() const { return static_cast<TypeId>(static_cast<uint8_t>(type_) & 0x3F); }

  Value() = delete;

  bool Null() const { return static_cast<bool>(static_cast<uint8_t>(type_) & 0x40); }

  void SetNull(const bool null) {
    if (null) {
      type_ = static_cast<TypeId>(static_cast<uint8_t>(type_) | 0x40);
    } else {
      type_ = static_cast<TypeId>(static_cast<uint8_t>(type_) & 0xBF);
    }
  }

 private:
  Value(const TypeId type, byte *const data) {
    std::memset(data_, 0, 16);
    if (data != nullptr) {
      type_ = type;
      std::memcpy(data_, &data, 8);
    } else {
      SetNull(true);
    }
    SetInlined(false);
  }

  template <typename T>
  Value(const TypeId type, T data) {
    std::memset(data_, 0, 16);
    type_ = type;
    const auto num_bytes = static_cast<size_t>(static_cast<uint8_t>(TypeUtil::GetTypeSize(type)) & 0x7F);
    TERRIER_ASSERT(num_bytes <= 16, "Too large to fit into data_ buffer.");
    std::memcpy(data_, &data, num_bytes);
    SetInlined(true);
  }

  template <typename T>
  T GetAs() const {
    if (Inlined()) return *reinterpret_cast<const T *const>(data_);
    return **reinterpret_cast<const T *const *const>(data_);
  }

  bool Inlined() const { return static_cast<bool>(static_cast<uint8_t>(type_) & 0x80); }

  void SetInlined(const bool inlined) {
    if (inlined) {
      type_ = static_cast<TypeId>(static_cast<uint8_t>(type_) | 0x80);
    } else {
      type_ = static_cast<TypeId>(static_cast<uint8_t>(type_) & 0x7F);
    }
  }

  TypeId type_ = TypeId::INVALID;
  byte data_[16];
};

}  // namespace terrier::type
