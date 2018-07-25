#pragma once

#include <cstdint>
#include <cstdlib>
#include <unordered_set>
#include "common/common_defs.h"
#include "common/spin_latch.h"

namespace terrier {

/**
 * A varlen pool that can quickly allocate chunks of memory.
 */
class VarlenPool {
 public:
  /**
   * Destructs the varlen pool. Frees any memory it holds.
   */
  ~VarlenPool() {
    pool_lock_.Lock();
    for (auto location : locations_) {
      delete[] location;
    }
    pool_lock_.Unlock();
  }
  /**
   * Allocate bytes of memory from the varlen pool
   * @param size The size of bytes to be allocated
   */
  void *Allocate(uint64_t size) {
    auto location = new byte[size];

    pool_lock_.Lock();
    locations_.insert(location);
    pool_lock_.Unlock();

    return location;
  }

  /**
   * Free some certain memory from the varlen pool
   * @param ptr The address of memory to be freed
   */
  void Free(void *ptr) {
    auto *cptr = (byte *)ptr;
    pool_lock_.Lock();
    auto result = locations_.erase(cptr);
    pool_lock_.Unlock();
    if (result) {
      delete[] cptr;
    }
  }

 public:
  /** Location list */
  std::unordered_set<byte *> locations_;

  /** Spin lock protecting location list */
  SpinLatch pool_lock_;
};

}  // namespace terrier
