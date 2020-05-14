#include "common/performance_counter.h"

namespace terrier {

// clang-format off
/**
 * A simple dummy cache object with four differently typed attributes:
 *   uint64_t NumInsert
 *   uint32_t NumHit
 *   uint16_t NumFailure
 *   uint8_t NumUser
 */
#define CACHE_MEMBERS(f) \
  f(uint64_t, NumInsert) \
  f(uint32_t, NumHit) \
  f(uint16_t, NumFailure) \
  f(uint8_t, NumUser)
// clang-format on

DEFINE_PERFORMANCE_CLASS_HEADER(CacheCounter, CACHE_MEMBERS)
}  // namespace terrier
