#include "execution/util/hash.h"

#include "xxh3.h"  // NOLINT

namespace terrier::execution::util {

hash_t Hasher::HashXX3(const u8 *buf, const u32 len) { return XXH3_64bits(buf, len); }

}  // namespace terrier::execution::util
