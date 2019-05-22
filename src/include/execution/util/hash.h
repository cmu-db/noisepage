#pragma once

#include <x86intrin.h>
#include <cstdint>
#include <type_traits>

#include "execution/util/common.h"
#include "execution/util/macros.h"

namespace tpl::util {

/// Enumeration of the supported hashing methods
enum class HashMethod { Fnv1, Murmur3, Crc };

/// Utility class for hashing
class Hasher {
 public:
  static hash_t Hash(const u8 *buf, u64 len, HashMethod method = HashMethod::Crc);

 private:
  static hash_t HashFnv1(const u8 *buf, u64 len);

  static hash_t HashMurmur3(const u8 *buf, u64 len);

  static hash_t HashCrc32(const u8 *buf, u64 len);
};

}  // namespace tpl::util
