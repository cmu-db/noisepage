#pragma once

#include <cstdint>
#include <type_traits>

#include "execution/util/common.h"
#include "execution/util/macros.h"

namespace tpl::util {

/**
 * Enumeration of the supported hashing methods
 */
enum class HashMethod : u8 { Fnv1, Murmur3, Crc, xxHash3 };

/**
 * Generic hashing utility class
 */
class Hasher {
 public:
  /**
   * Hash the given input buffer
   * @param buf The input buffer
   * @param len The length of the input buffer
   * @param method The hashing method to use (default CRC)
   * @return A hash value for the input
   */
  static hash_t Hash(const u8 *buf, u64 len, HashMethod method = HashMethod::Crc);

  /**
   * Combine and mix two hash values into a new hash value
   * @param first_hash The first hash value
   * @param second_hash The second hash value
   * @return The mixed hash value
   */
  static hash_t CombineHashes(const hash_t first_hash, const hash_t second_hash) {
    // Based on Hash128to64() from cityhash.
    static constexpr auto kMul = u64(0x9ddfea08eb382d69);
    hash_t a = (first_hash ^ second_hash) * kMul;
    a ^= (a >> 47u);
    hash_t b = (second_hash ^ a) * kMul;
    b ^= (b >> 47u);
    b *= kMul;
    return b;
  }

 private:
  static hash_t HashFnv1(const u8 *buf, u64 len);

  static hash_t HashMurmur3(const u8 *buf, u64 len);

  static hash_t HashCrc32(const u8 *buf, u64 len);

  static hash_t HashXXHash3(const u8 *buf, u64 len);
};

}  // namespace tpl::util
