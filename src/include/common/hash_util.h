#pragma once

#include <x86intrin.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <type_traits>

#include "common/macros.h"
#include "common/strong_typedef.h"
#include "execution/util/execution_common.h"
#include "xxHash/xxh3.h"

namespace terrier::common {

/**
 * This is our typedef that we use throughout the entire code to represent a hash value.
 */
using hash_t = uint64_t;

/**
 * Generic hashing utility class. The main entry point are the HashUtil::Hash() functions. There are
 * overloaded specialized versions for arithmetic values (integers and floats), and generic versions
 * for longer buffers (strings, c-strings, and opaque buffers).
 *
 * @warning If you change any of this functionality, compare stable performance numbers of varlen_entry_benchmark before
 * and after. It is not currently part of CI because it can be noisy.
 */
class EXPORT HashUtil {
 public:
  /** This class cannot be instantiated. */
  DISALLOW_INSTANTIATION(HashUtil);
  /** This class cannot be copied or moved. */
  DISALLOW_COPY_AND_MOVE(HashUtil);

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
   * Hash the given object by value.
   * @tparam T type to be hashed
   * @param obj object to be hashed
   * @return hash of object
   */
  template <typename T>
  static auto Hash(const T &obj) -> std::enable_if_t<
      !std::is_arithmetic_v<T> && !std::is_same<T, std::string>::value && !std::is_same<T, char>::value, hash_t> {
    return XXH3_64bits(reinterpret_cast<const byte *>(&obj), sizeof(T));
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
   * Adds two hashes together. Commutative. WATCH OUT WHEN USING.
   *  TODO(WAN): The optimizer uses this, but I'm wary of this function.
   *   I suspect that SumHashes(Hash(x), 0) != Hash(x).
   * @param l left hash
   * @param r right hash
   * @return sum of two hashes
   */
  static hash_t SumHashes(const hash_t l, const hash_t r) {
    static const hash_t prime_factor = 10000019;
    return (l % prime_factor + r % prime_factor) % prime_factor;
  }

  /**
   * Compute the hash value of an arithmetic input. The input is allowed to be either an integral number (8- to 64-bits)
   * or floating point number.
   * @tparam T The input arithmetic type.
   * @param val The input value to hash.
   * @param seed The seed hash value to mix in.
   * @return The computed hash.
   */
  template <typename T>
  static auto Hash(T val, hash_t seed) -> std::enable_if_t<std::is_arithmetic_v<T>, hash_t> {
    return HashCrc(val, seed);
  }

  /**
   * Compute the hash value of an arithmetic input. The input is allowed to be either an integral
   * numbers (8- to 64-bits) or floating pointer numbers.
   * @tparam T The input arithmetic type.
   * @param val The input value to hash.
   * @return The computed hash.
   */
  template <typename T>
  static auto Hash(T val) -> std::enable_if_t<std::is_arithmetic_v<T>, hash_t> {
    return HashCrc(val);
  }

  /**
   * Compute the hash value of the input buffer with the provided length.
   * @param buf The input buffer.
   * @param len The length of the input buffer to hash.
   * @return The computed hash value based on the contents of the input buffer.
   */
  static auto Hash(const uint8_t *buf, std::size_t len) -> hash_t { return HashXX3(buf, len); }

  /**
   * Compute the hash value of the input buffer with the provided length and using a seed hash.
   * @param buf The input buffer.
   * @param len The length of the input buffer to hash.
   * @param seed The seed hash value to mix in.
   * @return The computed hash value based on the contents of the input buffer.
   */
  static auto Hash(const uint8_t *buf, std::size_t len, hash_t seed) -> hash_t { return HashXX3(buf, len, seed); }

  /**
   * Special case Hash method for strings. If you use the above version (const T &obj),
   * you will hash the address of the string's data, which is not what you want.
   * @param s the string to be hashed
   * @return hash of the string
   */
  static auto Hash(const std::string &s) -> hash_t {
    return HashXX3(reinterpret_cast<const uint8_t *>(s.data()), s.size());
  }

  /**
   * Special case Hash method for string literals.
   * @param str the string to be hashed
   * @return hash of the string
   */
  static hash_t Hash(const char *str) { return Hash(std::string_view(str)); }

  /**
   * Compute the hash value of an input string view @em s.
   * @param s The input string.
   * @return The computed hash value based on the contents of the input string.
   */
  static auto Hash(const std::string_view s) -> hash_t {
    return HashXX3(reinterpret_cast<const uint8_t *>(s.data()), s.length());
  }

  /**
   * Combine and mix two hash values into a new hash value
   * @param first_hash The first hash value
   * @param second_hash The second hash value
   * @return The mixed hash value
   */
  static hash_t CombineHashes(const hash_t first_hash, const hash_t second_hash) {
    // Based on Hash128to64() from cityhash.xxh3
    static constexpr auto k_mul = uint64_t(0x9ddfea08eb382d69);
    hash_t a = (first_hash ^ second_hash) * k_mul;
    a ^= (a >> 47u);
    hash_t b = (second_hash ^ a) * k_mul;
    b ^= (b >> 47u);
    b *= k_mul;
    return b;
  }

  /**
   * Compute a new hash value that scrambles the bits in the input hash value. This function
   * guarantees that if h1 and h2 are two hash values, then scramble(h1) == scramble(h2).
   * @param hash The input hash value to scramble.
   * @return The scrambled hash value.
   */
  static hash_t ScrambleHash(const hash_t hash) { return XXH64_avalanche(hash); }

  /**
   * Integer CRC hashing based on HyPer.
   */
  template <typename T>
  static auto HashCrc(T val, hash_t seed) -> std::enable_if_t<std::is_fundamental_v<T>, hash_t> {
    // Thanks HyPer
    static constexpr hash_t k_default_crc_seed = 0x04c11db7ULL;

    uint64_t result1 = _mm_crc32_u64(seed, static_cast<uint64_t>(val));
    uint64_t result2 = _mm_crc32_u64(k_default_crc_seed, static_cast<uint64_t>(val));
    return ((result2 << 32u) | result1) * 0x2545f4914f6cdd1dULL;
  }

  /**
   * Integer CRC hashing based on HyPer with a seed of 0.
   */
  template <typename T>
  static auto HashCrc(T val) -> std::enable_if_t<std::is_fundamental_v<T>, hash_t> {
    return HashCrc(val, 0);
  }

  /**
   * String CRC hashing.
   */
  static hash_t HashCrc(const uint8_t *buf, uint32_t len, hash_t seed) {
    uint64_t hash = seed;

    // Process as many 8-byte chunks as possible.
    for (; len >= 8; buf += 8, len -= 8) {
      hash = HashCrc(*reinterpret_cast<const uint64_t *>(buf), hash);
    }

    // If there's at least a 4-byte chunk, process that.
    if (len >= 4) {
      hash = HashCrc(*reinterpret_cast<const uint32_t *>(buf), hash);
      buf += 4;
      len -= 4;
    }

    // Process the tail.
    switch (len) {
      case 3:
        hash ^= (static_cast<uint64_t>(buf[2])) << 16u;
        TERRIER_FALLTHROUGH;
      case 2:
        hash ^= (static_cast<uint64_t>(buf[1])) << 8u;
        TERRIER_FALLTHROUGH;
      case 1:
        hash ^= buf[0];
        TERRIER_FALLTHROUGH;
      default:
        break;
    }

    return hash;
  }

  /**
   * String CRC hashing with a seed of 0.
   */
  static hash_t HashCrc(const uint8_t *buf, uint32_t len) { return HashCrc(buf, len, 0); }

  /**
   * Integer Murmur3 hashing.
   */
  template <typename T>
  static auto HashMurmur(T val, hash_t seed) -> std::enable_if_t<std::is_arithmetic_v<T>, hash_t> {
    auto k = static_cast<uint64_t>(val);
    k ^= seed;
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdLLU;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53LLU;
    k ^= k >> 33;
    return k;
  }

  /**
   * Integer Murmur3 hashing with a seed of 0.
   */
  template <typename T>
  static auto HashMurmur(T val) -> std::enable_if_t<std::is_fundamental_v<T>, hash_t> {
    return HashMurmur(val, 0);
  }

  /**
   * String XXH3 hashing.
   */
  static hash_t HashXX3(const uint8_t *buf, uint32_t len, hash_t seed) { return XXH3_64bits_withSeed(buf, len, seed); }

  /**
   * String XXH3 hashing (no seed).
   */
  static hash_t HashXX3(const uint8_t *buf, uint32_t len) { return XXH3_64bits(buf, len); }

  /**
   * Arbitrary object XXH3 hashing.
   */
  template <typename T>
  static auto HashXX3(T val, hash_t seed) -> std::enable_if_t<std::is_arithmetic_v<T>, hash_t> {
    return XXH3_64bits_withSeed(&val, sizeof(T), seed);
  }

  /**
   * Arbitrary object XXH3 hashing (no seed).
   */
  template <typename T>
  static auto HashXX3(const T val) -> std::enable_if_t<std::is_arithmetic_v<T>, hash_t> {
    return XXH3_64bits(&val, sizeof(T));
  }
};

}  // namespace terrier::common
