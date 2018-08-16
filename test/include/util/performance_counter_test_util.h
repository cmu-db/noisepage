#pragma once
#include "common/performance_counter.h"

namespace terrier {
/**
 * A simple dummy cache object with four differently typed attributes:
 *   uint64_t num_insert
 *   uint32_t num_hit
 *   uint16_t num_failure
 *   uint8_t num_user
 */
#define CACHE_MEMBERS(f) f(uint64_t, num_insert) f(uint32_t, num_hit) f(uint16_t, num_failure) f(uint8_t, num_user)

MAKE_PERFORMANCE_COUNTER(CacheCounter, CACHE_MEMBERS)

/**
 * A simple dummy network object
 *   uint64_t num_requests
 */
#define NETWORK_MEMBERS(f) f(uint64_t, num_requests)

MAKE_PERFORMANCE_COUNTER(NetworkCounter, NETWORK_MEMBERS)
}  // namespace terrier
