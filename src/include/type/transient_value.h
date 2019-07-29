#pragma once

#include <algorithm>
#include <cstring>
#include <string>
#include "common/hash_util.h"
#include "common/json.h"
#include "common/macros.h"
#include "type/type_id.h"
#include "type/type_util.h"

namespace terrier::parser {
class ConstantValueExpression;
}

namespace terrier::type {
class TransientValueFactory;
class TransientValuePeeker;

/**
 * TransientValue objects are immutable containers for SQL type variables. They are intended to be used as a transport
 * class for values between the parser layer and the execution layer. TransientValue objects are created using the
 * TransientValueFactory, rather than exposing any constructors directly. This is to make it as obvious as possible if
 * the user is doing something that may hurt performance like repeatedly creating and destroying TransientValue objects.
 * C types are extracted from TransientValue objects using the TransientValuePeeker class.
 * @see TransientValueFactory
 * @see TransientValuePeeker
 */
class TransientValue {
  friend class TransientValueFactory;                     // Access to constructor
  friend class TransientValuePeeker;                      // Access to GetAs
  friend class terrier::parser::ConstantValueExpression;  // Access to copy constructor, json methods

 public:
  /**
   * @return TypeId of this TransientValue object.
   */
  TypeId Type() const {
    // bitwise AND the TypeId with 01111111 to return TypeId value without the embedded NULL bit
    return static_cast<TypeId>(static_cast<uint8_t>(type_) & 0x7F);
  }

  /**
   * @return true if TransientValue is a SQL NULL, otherwise false
   */
  bool Null() const {
    // bitwise AND the TypeId with 1000000 to extract NULL bit
    return static_cast<bool>(static_cast<uint8_t>(type_) & 0x80);
  }

  /**
   * Change the SQL NULL value of this TransientValue. We use the MSB to reflect this since we don't need all 8 bits for
   * TypeId
   * @param set_null true if TransientValue should be set to NULL, false otherwise
   */
  void SetNull(const bool set_null) {
    if (set_null) {
      // bitwise OR the TypeId with 1000000 to set NULL bit
      type_ = static_cast<TypeId>(static_cast<uint8_t>(type_) | 0x80);
    } else {
      // bitwise AND the TypeId with 01111111 to clear NULL bit
      type_ = static_cast<TypeId>(static_cast<uint8_t>(type_) & 0x7F);
    }
  }

  /**
   * VARCHAR's own their own internal buffer so we need to make sure to free it.
   */
  ~TransientValue() {
    if (Type() == TypeId::VARCHAR) {
      delete[] reinterpret_cast<char *const>(data_);
    }
  }

  /**
   * @param rhs TransientValue to compare this against
   * @return true if two TransientValues are equal, satisfying the following predicates:
   * 1) they share the same TypeId
   * 2) they share the same NULL value
   * 3) they share the same data_ value if non-VARCHARs, or their VARCHAR contents are the same if VARCHARS
   * false otherwise
   * @warning This is an object equality check, and does not follow SQL equality semantics (specifically, NULL
   * comparison)
   */
  bool operator==(const TransientValue &rhs) const {
    if (type_ != rhs.type_) return false;  // checks TypeId and NULL at the same time due to stolen MSB of type_ field
    if (type_ != TypeId::VARCHAR) return data_ == rhs.data_;

    const auto *const varchar = reinterpret_cast<const char *const>(data_);
    const uint32_t length = *reinterpret_cast<const uint32_t *const>(varchar);

    const auto *const rhs_varchar = reinterpret_cast<const char *const>(rhs.data_);
    const uint32_t rhs_length = *reinterpret_cast<const uint32_t *const>(rhs_varchar);

    if (length != rhs_length) return false;

    const char *const varchar_contents = varchar + sizeof(uint32_t);
    const char *const rhs_varchar_contents = rhs_varchar + sizeof(uint32_t);

    return std::memcmp(varchar_contents, rhs_varchar_contents, length) == 0;
  }

  /**
   * @param rhs TransientValue to compare this against
   * @return Negation of @see TransientValue::operator==
   * @warning This is an object equality check, and does not follow SQL equality semantics (specifically, NULL
   * comparison)
   */
  bool operator!=(const TransientValue &rhs) const { return !(operator==(rhs)); }

  /**
   * @return hash_t representing this TransientValue
   */
  common::hash_t Hash() const {
    const auto type_hash = common::HashUtil::Hash(type_);

    if (type_ != TypeId::VARCHAR) {
      const auto data_hash = common::HashUtil::Hash(data_);
      return common::HashUtil::CombineHashes(type_hash, data_hash);
    }

    const uint32_t length = *reinterpret_cast<const uint32_t *const>(data_);
    const auto data_hash =
        common::HashUtil::HashBytes(reinterpret_cast<const byte *const>(data_), length + sizeof(uint32_t));
    return common::HashUtil::CombineHashes(type_hash, data_hash);
  }

  /**
   * Move assignment operator that takes ownership of another TransientValue's varlen buffer, if allocated. Otherwise
   * just simple assignment. This is public because in theory it's not as expensive as other assignemnt operators, and
   * should be favored if needing to assign a TransientValue from another TransientValue, assuming you don't need the
   * other object anymore.
   * @param other TransientValue to be moved from. It will be left as a NULL Boolean
   * @return reference of object assigned to
   */
  TransientValue &operator=(TransientValue &&other) noexcept {
    if (this != &other) {  // self-assignment check expected
      if (Type() == TypeId::VARCHAR) {
        // free VARCHAR buffer
        delete[] reinterpret_cast<char *const>(data_);
      }
      // take ownership of other's contents
      type_ = other.type_;
      data_ = other.data_;
      // leave other in a valid state, let's just set it to a NULL boolean for fun
      other.data_ = 0;
      other.type_ = TypeId::BOOLEAN;
      other.SetNull(true);
    }
    return *this;
  }

  /**
   * Move constructor that takes ownership of another TransientValue's varlen buffer, if allocated. Otherwise
   * just simple assignment. This is public because in theory it's not as expensive as other constructors, and should be
   * favored if needing to construct a TransientValue from another TransientValue, assuming you don't need the other
   * object anymore.
   * @param other TransientValue to be moved from. It will be left as a NULL Boolean
   * @return new object assigned to
   */
  TransientValue(TransientValue &&other) noexcept {
    // take ownership of other's contents
    type_ = other.type_;
    data_ = other.data_;
    // leave other in a valid state, let's just set it to a NULL boolean for fun
    other.data_ = 0;
    other.type_ = TypeId::BOOLEAN;
    other.SetNull(true);
  }

  /**
   * @return string representation of the underlying type
   */
  std::string ToString() const { return TypeUtil::TypeIdToString(type_); }

  /**
   * @return transient value serialized to json
   * @warning this method is ONLY used for serialization and deserialization. It should NOT be used to peek at the
   * values
   */
  nlohmann::json ToJson() const {
    nlohmann::json j;
    j["type"] = type_;
    j["data"] = data_;
    if (Type() == TypeId::VARCHAR) {
      const uint32_t length = *reinterpret_cast<const uint32_t *const>(data_);
      auto varchar = std::string(reinterpret_cast<const char *const>(data_), length + sizeof(uint32_t));
      j["data"] = varchar;
    } else {
      j["data"] = data_;
    }
    return j;
  }

  /**
   * @param j json to deserialize
   * @warning this method is ONLY used for serialization and deserialization. It should NOT be used to peek at the
   * values
   */
  void FromJson(const nlohmann::json &j) {
    type_ = j.at("type").get<TypeId>();
    if (Type() == TypeId::VARCHAR) {
      data_ = 0;
      CopyVarChar(reinterpret_cast<const char *const>(j.at("data").get<std::string>().c_str()));

    } else {
      data_ = j.at("data").get<uintptr_t>();
    }
  }

  /**
   * Default constructor used for deserializing json
   */
  TransientValue() = default;

 private:
  // The following tests make sure that the private copy constructor and copy assignment operator work, so they need to
  // be friends of the TransientValue class.
  FRIEND_TEST(ValueTests, BooleanTest);
  FRIEND_TEST(ValueTests, TinyIntTest);
  FRIEND_TEST(ValueTests, SmallIntTest);
  FRIEND_TEST(ValueTests, IntegerTest);
  FRIEND_TEST(ValueTests, BigIntTest);
  FRIEND_TEST(ValueTests, DecimalTest);
  FRIEND_TEST(ValueTests, TimestampTest);
  FRIEND_TEST(ValueTests, DateTest);
  FRIEND_TEST(ValueTests, VarCharTest);

  /**
   * Constructor for NULL value
   * @param type type id
   */
  explicit TransientValue(const TypeId type) : type_(type), data_(0) { SetNull(true); }

  // The following tests make sure that json serialization  works, so they need to
  // be friends of the TransientValue class.
  FRIEND_TEST(ValueTests, BooleanJsonTest);
  FRIEND_TEST(ValueTests, TinyIntJsonTest);
  FRIEND_TEST(ValueTests, SmallIntJsonTest);
  FRIEND_TEST(ValueTests, IntegerJsonTest);
  FRIEND_TEST(ValueTests, BigIntJsonTest);
  FRIEND_TEST(ValueTests, DecimalJsonTest);
  FRIEND_TEST(ValueTests, TimestampJsonTest);
  FRIEND_TEST(ValueTests, DateJsonTest);
  FRIEND_TEST(ValueTests, VarCharJsonTest);

  template <typename T>
  TransientValue(const TypeId type, T data) {
    // clear internal buffer
    data_ = 0;
    type_ = type;
    const auto num_bytes = std::min(static_cast<uint8_t>(static_cast<uint8_t>(TypeUtil::GetTypeSize(type)) & 0x7F),
                                    static_cast<uint8_t>(sizeof(uintptr_t)));
    std::memcpy(&data_, &data, num_bytes);
  }

  /**
   * The copy constructor for TransientValue is currently only used by a single parser node, and should be used
   * sparingly due to its reliance on malloc/free. The new TransientValue will have the same contents as the other
   * TransientValue, but in its own memory buffers.
   * @param other TransientValue to copy from.
   * @warning No, seriously: don't go crazy with this thing because it will tank performance if the system calls malloc
   * too much. We saw this with the old Value system in Peloton when Value objects were created and destroyed too
   * frequently during query execution.
   */
  TransientValue(const TransientValue &other) {
    // clear internal buffer
    data_ = 0;
    type_ = other.type_;
    if (Type() != TypeId::VARCHAR || other.data_ == 0) {
      data_ = other.data_;
    } else {
      CopyVarChar(reinterpret_cast<const char *const>(other.data_));
    }
  }

  /**
   * The copy assignment operator for TransientValue is currently unused, and should be used
   * sparingly due to its reliance on malloc/free. The assigned to TransientValue will have the same contents as the
   * other TransientValue, but in its own memory buffers.
   * @param other TransientValue to copy from
   * @return reference of object assigned to
   * @warning No, seriously: don't go crazy with this thing because it will tank performance if the system calls malloc
   * too much. We saw this with the old Value system in Peloton when Value objects were created and destroyed too
   * frequently during query execution.
   */
  TransientValue &operator=(const TransientValue &other) {
    if (this != &other) {  // self-assignment check expected
      if (Type() == TypeId::VARCHAR && data_ != 0) {
        // free VARCHAR buffer
        delete[] reinterpret_cast<char *const>(data_);
      }
      // clear internal buffer
      data_ = 0;
      type_ = other.type_;
      if (Type() != TypeId::VARCHAR || other.data_ == 0) {
        data_ = other.data_;
      } else {
        CopyVarChar(reinterpret_cast<const char *const>(other.data_));
      }
    }
    return *this;
  }

  /**
   * Used by TransientValuePeeker to get the internal buffer of the TransientValue. Don't use directly.
   * @tparam T C type to generate a TransientValue from
   * @return reinterpreted version of the internal buffer based on the C type template parameter
   * @see: TransientValuePeeker for actual access to TransientValue's data as a C type
   */
  template <typename T>
  T GetAs() const {
    const auto *const value = reinterpret_cast<const T *const>(&data_);
    return *value;
  }

  /**
   * Helper method to take a null-terminated C string, allocate a VARCHAR buffer, copy the C string into the buffer, and
   * then assign the pointer to the TransientValue's internal data buffer.
   * @param other null-terminated C string to build a VARCHAR buffer for
   */
  void CopyVarChar(const char *const c_string) {
    TERRIER_ASSERT(Type() == TypeId::VARCHAR,
                   "This TransientValue's type should be set to VARCHAR if this function is being called.");
    // allocate a VARCHAR buffer
    const auto *const other_varchar = reinterpret_cast<const char *const>(c_string);
    const uint32_t length = *reinterpret_cast<const uint32_t *const>(other_varchar);
    auto *const varchar = new char[length + sizeof(uint32_t)];

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
  // TODO(Matt): we should consider padding 7 bytes to inline small varlens in the future if we want
  uintptr_t data_;
};

DEFINE_JSON_DECLARATIONS(TransientValue);

}  // namespace terrier::type
