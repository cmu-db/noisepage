#pragma once

#include <algorithm>
#include <cstdlib>
#include <string>
#include "common/strong_typedef.h"
namespace terrier::common {

/**
 * This is our typedef that we use throughout the entire code to represent a hash value.
 */
using hash_t = uint64_t;

/**
 * An utility class containing hash functions.
 */
class HashUtil {
 private:
  static const hash_t PRIME_FACTOR = 10000019;

 public:
  // Static utility class
  HashUtil() = delete;

  /**
   * Hashes length number of bytes.
   * Source:
   * https://github.com/greenplum-db/gpos/blob/b53c1acd6285de94044ff91fbee91589543feba1/libgpos/src/utils.cpp#L126
   *
   * @param bytes bytes to be hashed
   * @param length number of bytes
   * @return hash
   */
  static hash_t HashBytes(const byte *bytes, const uint64_t length) {
    hash_t hash = length;
    for (uint64_t i = 0; i < length; ++i) {
      hash = ((hash << 5) ^ (hash >> 27)) ^ static_cast<uint8_t>(bytes[i]);  // NOLINT
    }
    return hash;
  }

  /**
   * Combines two hashes together by hashing them again.
   * @param l left hash
   * @param r right hash
   * @return combined hash
   */
  static hash_t CombineHashes(const hash_t l, const hash_t r) {
    hash_t both[2];
    both[0] = l;
    both[1] = r;
    return HashBytes(reinterpret_cast<byte *>(both), sizeof(hash_t) * 2);
  }

  /**
   * Combine first to last items from the iterator to the base hash
   * @tparam IteratorType
   * @param base starting hash
   * @param first iterator start
   * @param last iterator end
   * @return combined hash
   */
  template <class IteratorType>
  static hash_t CombineHashInRange(const hash_t base, IteratorType first, IteratorType last) {
    hash_t result = base;
    for (; first != last; ++first) result = CombineHashes(result, Hash(*first));
    return result;
  }

  /**
   * Adds two hashes together. Commutative.
   * @param l left hash
   * @param r right hash
   * @return sum of two hashes
   */
  static hash_t SumHashes(const hash_t l, const hash_t r) {
    return (l % PRIME_FACTOR + r % PRIME_FACTOR) % PRIME_FACTOR;
  }

  /**
   * Hash the given object by value.
   * @tparam T type to be hashed
   * @param obj object to be hashed
   * @return hash of object
   */
  template <typename T>
  static hash_t Hash(const T &obj) {
    return HashBytes(reinterpret_cast<const byte *>(&obj), sizeof(T));
  }

  /**
   * Special case Hash method for strings. If you use the above version,
   * you will hash the address of the string's data, which is not what you want.
   * @param str the string to be hashed
   * @return hash of the string
   */
  static hash_t Hash(const std::string &str) {
    return HashBytes(reinterpret_cast<const byte *>(str.data()), str.size());
  }

  /**
   * Special case Hash method for string literals. This is dirty. We are
   * just going to wrap the character array that you give us into a new
   * std::string object.
   * @param str the string to be hashed
   * @return hash of the string
   */
  static hash_t Hash(const char *str) {
    // HACK HACK HACK
    return Hash(std::string(str));
  }
};

}  // namespace terrier::common
