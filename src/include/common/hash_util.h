#pragma once

#include <algorithm>
#include <cstdlib>
#include <string>

namespace terrier {

using hash_t = std::size_t;

/**
 * An utility class containing hash functions.
 */
class HashUtil {
 private:
  static const hash_t prime_factor = 10000019;

 public:
  /**
   * Hashes length number of bytes.
   * Source:
   * https://github.com/greenplum-db/gpos/blob/b53c1acd6285de94044ff91fbee91589543feba1/libgpos/src/utils.cpp#L126
   *
   * @param bytes bytes to be hashed
   * @param length number of bytes
   * @return hash
   */
  static inline hash_t HashBytes(const char *bytes, const size_t length) {
    hash_t hash = length;
    for (size_t i = 0; i < length; ++i) {
      hash = ((hash << 5) ^ (hash >> 27)) ^ bytes[i];
    }
    return hash;
  }

  /**
   * Combines two hashes together by hashing them again.
   * @param l left hash
   * @param r right hash
   * @return combined hash
   */
  static inline hash_t CombineHashes(const hash_t l, const hash_t r) {
    hash_t both[2];
    both[0] = l;
    both[1] = r;
    return HashBytes(reinterpret_cast<char *>(both), sizeof(hash_t) * 2);
  }

  /**
   * Adds two hashes together. Commutative.
   * @param l left hash
   * @param r right hash
   * @return sum of two hashes
   */
  static inline hash_t SumHashes(const hash_t l, const hash_t r) {
    return (l % prime_factor + r % prime_factor) % prime_factor;
  }

  /**
   * Hash what pointer ptr is pointing to.
   * @tparam T type to be hashed
   * @param ptr pointer to object to be hashed
   * @return hash of object
   */
  template <typename T>
  static inline hash_t Hash(const T *ptr) {
    return HashBytes(reinterpret_cast<const char *>(ptr), sizeof(T));
  }

  /**
   * Hash the pointer ptr itself.
   * @tparam T type of pointer to be hashed
   * @param ptr pointer to be hashed
   * @return hash of pointer
   */
  template <typename T>
  static inline hash_t HashPtr(const T *ptr) {
    return HashBytes(static_cast<char *>(&ptr), sizeof(void *));
  }
};

}  // namespace terrier
