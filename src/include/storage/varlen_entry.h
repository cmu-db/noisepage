#pragma once

#include <algorithm>
#include <cstring>
#include <vector>

#include "common/hash_util.h"

namespace terrier::storage {
/**
 * A varlen entry is always a 32-bit size field and the varlen content,
 * with exactly size many bytes (no extra nul in the end).
 */
class VarlenEntry {
 public:
  /**
   * Constructs a new varlen entry. The varlen entry will take ownership of the pointer given if reclaimable is true,
   * which means GC can delete the buffer pointed to when this entry is no longer visible in the storage engine.
   * @param content pointer to the varlen content itself
   * @param size length of the varlen content, in bytes (no C-style nul-terminator)
   * @param reclaim whether the varlen entry's content pointer can be deleted by itself. If the pointer was not
   *                    allocated by itself (e.g. inlined, or part of a dictionary batch or arrow buffer), it cannot
   *                    be freed by the GC, which simply calls delete.
   * @return constructed VarlenEntry object
   */
  static VarlenEntry Create(const byte *content, uint32_t size, bool reclaim) {
    VarlenEntry result;
    if (size <= InlineThreshold()) {
      TERRIER_ASSERT(!reclaim, "can't reclaim this");
      return CreateInline(content, size);
    }
    TERRIER_ASSERT(size > InlineThreshold(), "small varlen values should be inlined");
    result.size_ = reclaim ? size : (INT32_MIN | size);  // the first bit denotes whether we can reclaim it
    std::memcpy(result.prefix_, content, sizeof(uint32_t));
    result.content_ = content;
    return result;
  }

  /**
   * Construct a new varlen entry whose contents match the provided string. The varlen DOES NOT
   * take ownership of the content, but it will store a pointer to it if it cannot apply a small
   * string optimization. It is the caller's responsibility to ensure the content outlives this
   * varlen entry.
   *
   * @param str The input string.
   * @return A constructed VarlenEntry object.
   */
  static VarlenEntry Create(std::string_view str) {
    if (str.length() > InlineThreshold()) {
      return Create(reinterpret_cast<const byte *>(str.data()), str.length(), false);
    }
    return CreateInline(reinterpret_cast<const byte *>(str.data()), str.length());
  }

  /**
   * Constructs a new varlen entry, with the associated varlen value inlined within the struct itself. This is only
   * possible when the inlined value is smaller than InlineThreshold() as defined. The value is copied and the given
   * pointer can be safely deallocated regardless of the state of the system.
   * @param content pointer to the varlen content
   * @param size length of the varlen content, in bytes (no C-style nul-terminator. Must be smaller than
   *             InlineThreshold())
   * @return constructed VarlenEntry object
   */
  static VarlenEntry CreateInline(const byte *content, uint32_t size) {
    TERRIER_ASSERT(size <= InlineThreshold(), "varlen value must be small enough for inlining to happen");
    VarlenEntry result;
    result.size_ = size;
    // Small string: just store the prefix as part of inline storage.
    // But first zero initialize prefix to ensure strings smaller than GetPrefixSize() still have an equal prefix.
    std::memset(result.prefix_, 0, PrefixSize());
    if (size != 0) {
      std::memcpy(result.prefix_, content, size);
    }
    return result;
  }

  /**
   * @return The maximum size of the varlen field, in bytes, that can be inlined within the object. Any objects that are
   * larger need to be stored as a pointer to a separate buffer.
   */
  static constexpr uint32_t InlineThreshold() { return sizeof(VarlenEntry) - sizeof(uint32_t); }

  /**
   * @return length of the prefix of the varlen stored in the object for execution engine, if the varlen entry is not
   * inlined.
   */
  static constexpr uint32_t PrefixSize() { return sizeof(uint32_t); }

  /**
   * @return size of the varlen value stored in this entry, in bytes.
   */
  uint32_t Size() const { return static_cast<uint32_t>(INT32_MAX & size_); }

  /**
   * @return whether the content is inlined or not.
   */
  bool IsInlined() const { return Size() <= InlineThreshold(); }

  /**
   * Helper method to decide if the content needs to be GCed separately
   * @return whether the content can be deallocated by itself
   */
  bool NeedReclaim() const {
    // force a signed comparison, if our sign bit is set size_ is negative so the test returns false
    return size_ > static_cast<int32_t>(InlineThreshold());
  }

  /**
   * @return pointer to the stored prefix of the varlen entry
   */
  const byte *Prefix() const { return prefix_; }

  /**
   * @return pointer to the varlen entry contents.
   */
  const byte *Content() const { return IsInlined() ? prefix_ : content_; }

  /**
   * @return zero-copy view of the VarlenEntry as an immutable string that allows use with convenient STL functions
   * @warning It is the programmer's responsibility to ensure that std::string_view does not outlive the VarlenEntry
   */
  std::string_view StringView() const {
    return std::string_view(reinterpret_cast<const char *const>(Content()), Size());
  }

  /**
   * Deserializes all elements of type T into a returned vector from this varlen
   * @tparam T type of elements that are serialized into this varlen entry
   * @return a vector of immutable deserialized T objects from this varlen entry
   * @warning It is the programmer's responsibility to ensure that the returned vector doesn't outlive the VarlenEntry
   */
  template <typename T>
  std::vector<T> DeserializeArray() const {
    const byte *contents = Content();
    size_t num_elements = *reinterpret_cast<const size_t *>(contents);
    TERRIER_ASSERT(sizeof(T) == (Size() - sizeof(size_t)) / num_elements,
                   "Deserializing the wrong element types from array");
    const T *payload = reinterpret_cast<const T *>(contents + sizeof(size_t));
    return std::vector<T>(payload, payload + num_elements);
  }

  // TODO(tanujnay112): Generalize to return other read-only varlen types
  /**
   * Deserializes all elements of std::string type into a returned vector from this varlen
   * @return a vector of string_view objects from this varlen entry
   * @warning It is the programmer's responsibility to ensure that the returned vector doesn't outlive the VarlenEntry
   * @warning Assuming this varlen is serialized in the format specified by
   * StorageUtils::CreateVarlen(const std::vector<const std::string> &vec)
   */
  std::vector<std::string_view> DeserializeArrayVarlen() const {
    std::vector<std::string_view> vec;
    uint32_t to_read = Size();
    uint32_t num_read = 0;

    const char *contents = reinterpret_cast<const char *>(Content());
    size_t num_elements = *reinterpret_cast<const size_t *>(contents);
    const char *payload = contents + sizeof(size_t);

    size_t length;
    while (num_read < to_read && vec.size() < num_elements) {
      length = *reinterpret_cast<const size_t *>(payload);
      payload += sizeof(size_t);
      vec.emplace_back(payload, length);
      payload += length;
      num_read += sizeof(size_t) + length;
    }

    return vec;
  }

  /**
   * @return The hash value of this variable-length string.
   */
  hash_t Hash() const { return Hash(0); }

  /**
   * Compute the hash value of this variable-length string instance.
   * @param seed The value to seed the hash with.
   * @return The hash value for this string instance.
   */
  hash_t Hash(hash_t seed) const {
    // "small" strings use CRC hashing, "long" strings use XXH3.
    if (IsInlined()) {
      return common::HashUtil::HashCrc(reinterpret_cast<const uint8_t *>(Prefix()), Size(), seed);
    }
    return common::HashUtil::HashXX3(reinterpret_cast<const uint8_t *>(Content()), Size(), seed);
  }

  /**
   * Compare two strings ONLY for equality or inequality only.
   * @tparam EqualCheck True for ==, false for !=.
   * @param left The first string.
   * @param right The second string.
   * @return 0 if equal according to EqualCheck; any non-zero value otherwise.
   */
  template <bool EqualityCheck>
  static bool CompareEqualOrNot(const VarlenEntry &left, const VarlenEntry &right) {
    TERRIER_ASSERT(left.Size() >= 0, "Left VarlenEntry has negative size?");
    TERRIER_ASSERT(right.Size() >= 0, "Right VarlenEntry has negative size?");

    // Compare the size and prefix in one fell swoop, ignoring the sign bit indicating reclaimability.
    uint8_t left_first = *reinterpret_cast<const uint8_t *>(&left);
    uint8_t right_first = *reinterpret_cast<const uint8_t *>(&right);
    bool first_byte_same = (left_first << 1) == (right_first << 1);
    bool following_bytes_same =
        0 == std::memcmp(reinterpret_cast<const char *>(&left) + 1, reinterpret_cast<const char *>(&right) + 1,
                         sizeof(left.size_) + PrefixSize() - 1);
    if (first_byte_same && following_bytes_same) {
      // Prefix and length are equal.
      if (left.IsInlined()) {
        if (std::memcmp(left.prefix_, right.prefix_, PrefixSize()) == 0) {
          return EqualityCheck ? true : false;
        }
      } else {
        if (std::memcmp(left.content_, right.content_, left.Size()) == 0) {
          return EqualityCheck ? true : false;
        }
      }
    }
    // Not equal.
    return EqualityCheck ? false : true;
  }

  /**
   * Compare two strings. Returns:
   * < 0 if left < right
   *  0  if left == right
   * > 0 if left > right
   *
   * @param left The first string.
   * @param right The second string.
   * @return The appropriate signed value indicating comparison order.
   */
  static int32_t Compare(const VarlenEntry &left, const VarlenEntry &right) {
    const auto min_len = std::min(left.Size(), right.Size());
    const auto result = std::memcmp(left.Content(), right.Content(), min_len);
    return result != 0 ? result : left.Size() - right.Size();
  }

  /**
   * @return True if this varlen equals @em that varlen; false otherwise.
   */
  bool operator==(const VarlenEntry &that) const { return CompareEqualOrNot<true>(*this, that); }

  /**
   * @return True if this varlen equals @em that varlen; false otherwise.
   */
  bool operator!=(const VarlenEntry &that) const { return CompareEqualOrNot<false>(*this, that); }

  /**
   * @return True if this varlen equals @em that varlen; false otherwise.
   */
  bool operator<(const VarlenEntry &that) const { return Compare(*this, that) < 0; }

  /**
   * @return True if this varlen equals @em that varlen; false otherwise.
   */
  bool operator<=(const VarlenEntry &that) const { return Compare(*this, that) <= 0; }

  /**
   * @return True if this varlen equals @em that varlen; false otherwise.
   */
  bool operator>(const VarlenEntry &that) const { return Compare(*this, that) > 0; }

  /**
   * @return True if this varlen equals @em that varlen; false otherwise.
   */
  bool operator>=(const VarlenEntry &that) const { return Compare(*this, that) >= 0; }

 private:
  int32_t size_;                   // buffer reclaimable => sign bit is 0 or size <= InlineThreshold
  byte prefix_[sizeof(uint32_t)];  // Explicit padding so that we can use these bits for inlined values or prefix
  const byte *content_;            // pointer to content of the varlen entry if not inlined
};
// To make sure our explicit padding is not screwing up the layout
static_assert(sizeof(VarlenEntry) == 16, "size of the class should be 16 bytes");

/**
 * Equality checker that checks the underlying varlen bytes are equal (deep)
 */
struct VarlenContentDeepEqual {
  /**
   *
   * @param lhs left hand side of comparison
   * @param rhs right hand side of comparison
   * @return whether the two varlen entries hold the same underlying value
   */
  bool operator()(const VarlenEntry &lhs, const VarlenEntry &rhs) const {
    return VarlenEntry::CompareEqualOrNot<true>(lhs, rhs);
  }
};

/**
 * Hasher that hashes the entry using the underlying varlen value
 */
struct VarlenContentHasher {
  /**
   * @param obj object to hash
   * @return hash code of object
   */
  size_t operator()(const VarlenEntry &obj) const { return obj.Hash(0); }
};

/**
 * Lexicographic comparison of two varlen entries.
 */
struct VarlenContentCompare {
  /**
   *
   * @param lhs left hand side of comparison
   * @param rhs right hand side of comparison
   * @return whether lhs < rhs in lexicographic order
   */
  bool operator()(const VarlenEntry &lhs, const VarlenEntry &rhs) const { return VarlenEntry::Compare(lhs, rhs) < 0; }
};

}  // namespace terrier::storage
