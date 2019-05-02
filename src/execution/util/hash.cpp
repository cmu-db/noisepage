#include "execution/util/hash.h"

#include <x86intrin.h>

#include "execution/util/macros.h"

namespace tpl::util {

hash_t Hasher::Hash(const u8 *buf, uint64_t len, HashMethod method) {
  switch (method) {
    case HashMethod::Fnv1:
      return HashFnv1(buf, len);
    case HashMethod::Murmur3:
      return HashMurmur3(buf, len);
    default:
      return HashCrc32(buf, len);
  }
}

hash_t Hasher::HashFnv1(const u8 *buf, uint64_t len) {
  auto hash = hash_t(2166136261ull);

  for (u64 i = 0; i < len; i++) {
    hash ^= buf[i];
    hash *= 16777619;
  }

  return hash;
}

hash_t Hasher::HashMurmur3(const u8 *buf, uint64_t len) { return 0; }

hash_t Hasher::HashCrc32(const u8 *buf, uint64_t len) {
  // Thanks HyPer
  auto gen_hash_64 = [](u64 input, u64 seed) {
    u64 result1 = _mm_crc32_u64(seed, input);
    u64 result2 = _mm_crc32_u64(0x04C11DB7, input);
    return ((result2 << 32) | result1) * 0x2545F4914F6CDD1Dull;
  };

  u64 hash = 0;

  // Process as many 8-byte chunks as possible
  for (; len >= 8; buf += 8, len -= 8) {
    hash = gen_hash_64(*reinterpret_cast<const u64 *>(buf), hash);
  }

  // If there's at least a 4-byte chunk, process that
  if (len >= 4) {
    hash = gen_hash_64(*reinterpret_cast<const u32 *>(buf), hash);
    buf += 4;
    len -= 4;
  }

  // Process the tail
  switch (len) {
    case 3:
      hash ^= (static_cast<u64>(buf[2])) << 16;
      FALLTHROUGH;
    case 2:
      hash ^= (static_cast<u64>(buf[1])) << 8;
      FALLTHROUGH;
    case 1:
      hash ^= buf[0];
      FALLTHROUGH;
    default:
      break;
  }

  return hash;
}

}  // namespace tpl::util
