#include "common/hash_util.h"

#include "xxHash/xxh3.h"

namespace terrier::common {

hash_t HashUtil::ScrambleHash(const hash_t hash) { return XXH64_avalanche(hash); }

hash_t HashUtil::HashXX3(const uint8_t *buf, uint32_t len, hash_t seed) { return XXH3_64bits_withSeed(buf, len, seed); }

hash_t HashUtil::HashXX3(const uint8_t *buf, uint32_t len) { return XXH3_64bits(buf, len); }

}  // namespace terrier::common
