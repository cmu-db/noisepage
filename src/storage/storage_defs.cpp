#include "storage/storage_defs.h"

#include "common/hash_util.h"
#include "common/strong_typedef_body.h"

namespace terrier::storage {

STRONG_TYPEDEF_BODY(col_id_t, uint16_t);
STRONG_TYPEDEF_BODY(layout_version_t, uint16_t);

hash_t VarlenEntry::Hash(hash_t seed) const {
  // "small" strings use CRC hashing, "long" strings use XXH3.
  if (IsInlined()) {
    return common::HashUtil::HashCrc(reinterpret_cast<const uint8_t *>(Prefix()), Size(), seed);
  }
  return common::HashUtil::HashXX3(reinterpret_cast<const uint8_t *>(Content()), Size(), seed);
}

}  // namespace terrier::storage
