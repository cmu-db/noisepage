#pragma once

#include <cstdint>
#include <cstdlib>
#include <unordered_set>
#include "common/spin_latch.h"
#include "common/typedefs.h"

namespace terrier::storage {

/**
 * A varlen entry is always a 32-bit size field and the varlen content,
 * with exactly size many bytes (no extra nul in the end)
 */
struct VarlenEntry {
  /**
   * Size of the varlen entry.
   */
  uint32_t size_;
  /**
   * Contents of the varlen entry.
   */
  byte content_[0];
};

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
    for (auto location : locations_) delete[] reinterpret_cast<byte *>(location);
    pool_lock_.Unlock();
  }
  /**
   * Allocate bytes of memory from the varlen pool
   * @param size The size of bytes to be allocated
   */
  VarlenEntry *Allocate(const uint32_t size) {
    auto *entry = reinterpret_cast<VarlenEntry *>(new byte[size + sizeof(uint32_t)]);
    entry->size_ = size;
    pool_lock_.Lock();
    locations_.insert(entry);
    pool_lock_.Unlock();
    return entry;
  }

  /**
   * Free some certain memory from the varlen pool
   * @param ptr The address of memory to be freed
   */
  void Free(VarlenEntry *const ptr) {
    pool_lock_.Lock();
    auto result = locations_.erase(ptr);
    pool_lock_.Unlock();
    if (result != 0u) delete[] reinterpret_cast<byte *>(ptr);
  }

 public:
  /** Location list */
  std::unordered_set<VarlenEntry *> locations_;

  /** Spin lock protecting location list */
  common::SpinLatch pool_lock_;
};

}  // namespace terrier::storage
