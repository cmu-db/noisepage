#include "execution/util/hash.h"

#include "xxh3.h"  // NOLINT

namespace tpl::util {

hash_t Hasher::HashXX3(const u8 *buf, const u32 len) { return XXH3_64bits(buf, len); }

}  // namespace tpl::util
