#include "execution/util/hash.h"

#include "xxHash/xxh3.h"

namespace terrier::execution::util {

hash_t Hasher::HashXX3(const uint8_t *buf, const uint32_t len) { return XXH3_64bits(buf, len); }

}  // namespace terrier::execution::util
